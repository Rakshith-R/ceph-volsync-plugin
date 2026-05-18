/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"google.golang.org/grpc"
)

// ---------------------------------------------------------------------------
// Mock infrastructure
// ---------------------------------------------------------------------------

// testReader is a DataReader backed by a byte slice. Safe for concurrent use.
type testReader struct {
	data []byte
}

func (r *testReader) ReadAt(_ string, offset, length int64) ([]byte, error) {
	if offset < 0 || offset > int64(len(r.data)) {
		return nil, fmt.Errorf("offset %d out of range [0, %d]", offset, len(r.data))
	}
	end := min(offset+length, int64(len(r.data)))
	out := make([]byte, end-offset)
	copy(out, r.data[offset:end])
	return out, nil
}

func (r *testReader) CloseFile(_ string) error { return nil }

// errorReader fails with errRead on the Nth call.
type errorReader struct {
	data    []byte
	failAt  int
	callsMu sync.Mutex
	calls   int
}

var errRead = errors.New("injected read error")

func (r *errorReader) ReadAt(_ string, offset, length int64) ([]byte, error) {
	r.callsMu.Lock()
	n := r.calls
	r.calls++
	r.callsMu.Unlock()

	if n >= r.failAt {
		return nil, errRead
	}
	end := min(offset+length, int64(len(r.data)))
	out := make([]byte, end-offset)
	copy(out, r.data[offset:end])
	return out, nil
}

func (r *errorReader) CloseFile(_ string) error { return nil }

// testIter is a BlockIterator over a static slice.
type testIter struct {
	blocks []ChangeBlock
	idx    int
}

func (it *testIter) Next() (*ChangeBlock, bool) {
	if it.idx >= len(it.blocks) {
		return nil, false
	}
	b := &it.blocks[it.idx]
	it.idx++
	return b, true
}

func (it *testIter) Close() error { return nil }

// ackStream properly acknowledges every sent block. This is critical
// for testing the window release path through ackReceiver.
type ackStream struct {
	grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse]
	mu       sync.Mutex
	cond     *sync.Cond
	pending  []uint64
	closed   bool
	sent     []*apiv1.WriteRequest
	sendErr  error
	recvErr  error
	sendHook func(req *apiv1.WriteRequest) // called under lock after appending
}

func newAckStream() *ackStream {
	s := &ackStream{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *ackStream) Send(req *apiv1.WriteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent = append(s.sent, req)
	for _, b := range req.Blocks {
		s.pending = append(s.pending, b.RequestId)
	}
	if s.sendHook != nil {
		s.sendHook(req)
	}
	s.cond.Signal()
	return nil
}

func (s *ackStream) Recv() (*apiv1.WriteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.pending) == 0 && !s.closed {
		s.cond.Wait()
	}
	if s.recvErr != nil {
		return nil, s.recvErr
	}
	if len(s.pending) == 0 {
		return nil, io.EOF
	}
	ids := make([]uint64, len(s.pending))
	copy(ids, s.pending)
	s.pending = s.pending[:0]
	return &apiv1.WriteResponse{AcknowledgedIds: ids}, nil
}

func (s *ackStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.cond.Signal()
	return nil
}

// getSent returns a copy of all sent requests.
func (s *ackStream) getSent() []*apiv1.WriteRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*apiv1.WriteRequest, len(s.sent))
	copy(out, s.sent)
	return out
}

// eofStream returns EOF immediately from Recv (no ack).
// Window slots are never released via ackReceiver.
type eofStream struct {
	grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse]
	mu   sync.Mutex
	sent []*apiv1.WriteRequest
}

func (s *eofStream) Send(req *apiv1.WriteRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, req)
	return nil
}

func (s *eofStream) Recv() (*apiv1.WriteResponse, error) { return nil, io.EOF }
func (s *eofStream) CloseSend() error                    { return nil }

// sendErrStream fails on the Nth Send call.
type sendErrStream struct {
	grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse]
	failAt int
	calls  atomic.Int32
}

var errSend = errors.New("injected send error")

func (s *sendErrStream) Send(_ *apiv1.WriteRequest) error {
	n := int(s.calls.Add(1)) - 1
	if n >= s.failAt {
		return errSend
	}
	return nil
}

func (s *sendErrStream) Recv() (*apiv1.WriteResponse, error) { return nil, io.EOF }
func (s *sendErrStream) CloseSend() error                    { return nil }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func makeBlocks(n int, chunkSize int64) []ChangeBlock { //nolint:unparam
	blocks := make([]ChangeBlock, n)
	for i := range n {
		blocks[i] = ChangeBlock{
			FilePath: testBlockDevice,
			Offset:   int64(i) * chunkSize,
			Len:      chunkSize,
			ReqID:    uint64(i),
		}
	}
	return blocks
}

func makeData(size int, pattern byte) []byte {
	d := make([]byte, size)
	for i := range d {
		d[i] = pattern
	}
	return d
}

func minCfg(chunkSize int64) Config { //nolint:unparam
	return Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       2,
		DataSendWorkers:   1,
		MaxWindow:         64,
		MaxRawMemoryBytes: 128 * 1024 * 1024,
	}
}

func collectBlocks(reqs []*apiv1.WriteRequest) []*apiv1.ChangedBlock {
	var out []*apiv1.ChangedBlock
	for _, r := range reqs {
		out = append(out, r.Blocks...)
	}
	return out
}

// ---------------------------------------------------------------------------
// End-to-end tests with proper acking
// ---------------------------------------------------------------------------

func TestPipeline_EndToEnd_WithAcking(t *testing.T) {
	chunkSize := int64(64 * 1024)
	n := 8
	data := makeData(n*int(chunkSize), 0xAA)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	blocks := collectBlocks(stream.getSent())
	if len(blocks) != n {
		t.Fatalf("expected %d blocks, got %d", n, len(blocks))
	}
}

func TestPipeline_DataIntegrity(t *testing.T) {
	chunkSize := int64(64 * 1024)
	n := 4
	data := make([]byte, n*int(chunkSize))
	for i := range data {
		data[i] = byte(i % 251) // prime modulo for non-repeating pattern
	}
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)
	cfg.DataSendWorkers = 1 // single worker for deterministic ordering

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	blocks := collectBlocks(stream.getSent())
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Offset < blocks[j].Offset
	})

	for _, b := range blocks {
		if b.IsZero {
			continue
		}
		off := int(b.Offset)
		expected := data[off : off+len(b.Data)]
		for i, v := range b.Data {
			if v != expected[i] {
				t.Fatalf("data mismatch at block offset %d, byte %d: got %02x want %02x", b.Offset, i, v, expected[i])
			}
		}
	}
}

func TestPipeline_MixedZeroAndNonZero(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := make([]byte, 4*int(chunkSize))
	// chunk 0: non-zero
	for i := 0; i < int(chunkSize); i++ {
		data[i] = 0xBB
	}
	// chunk 1: all zero (default)
	// chunk 2: non-zero
	for i := 2 * int(chunkSize); i < 3*int(chunkSize); i++ {
		data[i] = 0xCC
	}
	// chunk 3: all zero (default)

	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(4, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)
	cfg.DataSendWorkers = 1

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	blocks := collectBlocks(stream.getSent())
	if len(blocks) != 4 {
		t.Fatalf("expected 4 blocks, got %d", len(blocks))
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].RequestId < blocks[j].RequestId
	})

	if blocks[0].IsZero {
		t.Fatal("block 0 should not be zero")
	}
	if !blocks[1].IsZero {
		t.Fatal("block 1 should be zero")
	}
	if blocks[2].IsZero {
		t.Fatal("block 2 should not be zero")
	}
	if !blocks[3].IsZero {
		t.Fatal("block 3 should be zero")
	}

	// Zero blocks must have nil/empty data
	if len(blocks[1].Data) != 0 {
		t.Fatalf("zero block should have no data, got %d bytes", len(blocks[1].Data))
	}
	if len(blocks[3].Data) != 0 {
		t.Fatalf("zero block should have no data, got %d bytes", len(blocks[3].Data))
	}
}

func TestPipeline_SingleChunk(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := makeData(int(chunkSize), 0xDD)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(1, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	blocks := collectBlocks(stream.getSent())
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}
}

func TestPipeline_AllZeroChunks(t *testing.T) {
	chunkSize := int64(64 * 1024)
	n := 4
	data := make([]byte, n*int(chunkSize)) // all zeros
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	blocks := collectBlocks(stream.getSent())
	if len(blocks) != n {
		t.Fatalf("expected %d blocks, got %d", n, len(blocks))
	}
	for _, b := range blocks {
		if !b.IsZero {
			t.Fatalf("block reqID=%d should be zero", b.RequestId)
		}
	}
}

// ---------------------------------------------------------------------------
// Error propagation tests
// ---------------------------------------------------------------------------

func TestPipeline_ReaderError(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := makeData(4*int(chunkSize), 0xAA)
	reader := &errorReader{data: data, failAt: 2}
	iter := &testIter{blocks: makeBlocks(4, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err == nil {
		t.Fatal("expected error from reader")
	}
	if !errors.Is(err, errRead) {
		t.Fatalf("expected errRead, got: %v", err)
	}
}

func TestPipeline_StreamFactoryError(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := makeData(int(chunkSize), 0xAA)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(1, chunkSize)}

	errFactory := errors.New("connection refused")
	cfg := minCfg(chunkSize)

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return nil, errFactory
		}, win)
	if err == nil {
		t.Fatal("expected error from stream factory")
	}
	if !errors.Is(err, errFactory) {
		t.Fatalf("expected factory error, got: %v", err)
	}
}

func TestPipeline_StreamSendError(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := makeData(4*int(chunkSize), 0xAA)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(4, chunkSize)}

	stream := &sendErrStream{failAt: 0}
	cfg := minCfg(chunkSize)
	cfg.DataSendWorkers = 1

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err == nil {
		t.Fatal("expected error from stream send")
	}
	if !errors.Is(err, errSend) {
		t.Fatalf("expected errSend, got: %v", err)
	}
}

func TestPipeline_StreamRecvError(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := makeData(2*int(chunkSize), 0xAA)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(2, chunkSize)}

	errRecv := errors.New("connection reset")
	stream := newAckStream()
	// Inject recv error on the second Send so the first ack is
	// delivered normally, then Recv returns the error.
	var sendCount atomic.Int32
	stream.sendHook = func(_ *apiv1.WriteRequest) {
		if sendCount.Add(1) >= 2 {
			stream.recvErr = errRecv
		}
	}

	cfg := minCfg(chunkSize)
	// Force each chunk into a separate batch/Send call
	cfg.DataBatchMaxBytes = 1
	cfg.DataBatchMaxCount = 4
	cfg.DataSendWorkers = 1

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err == nil {
		t.Fatal("expected error from stream recv")
	}
	if !errors.Is(err, errRecv) {
		t.Fatalf("expected recv error, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Context cancellation tests
// ---------------------------------------------------------------------------

func TestPipeline_ContextCancel(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := makeData(100*int(chunkSize), 0xAA)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(100, chunkSize)}

	// Use eofStream (no acks) with tiny window so pipeline blocks
	// on window acquire, giving cancel time to propagate.
	stream := &eofStream{}
	cfg := minCfg(chunkSize)
	cfg.MaxWindow = 8

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel shortly after start via a goroutine.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(ctx, iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}

func TestPipeline_ContextTimeout(t *testing.T) {
	chunkSize := int64(64 * 1024)
	// Large data so pipeline won't finish before timeout
	data := makeData(200*int(chunkSize), 0xAA)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(200, chunkSize)}

	// Slow stream that never acks — will fill window and block
	slowStream := &eofStream{}
	cfg := minCfg(chunkSize)
	cfg.MaxWindow = 8 // small window to cause blocking

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(ctx, iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return slowStream, nil
		}, win)
	if err == nil {
		t.Fatal("expected timeout error")
	}
}

// ---------------------------------------------------------------------------
// Batch enforcement tests
// ---------------------------------------------------------------------------

func TestPipeline_DataBatchMaxBytes(t *testing.T) {
	chunkSize := int64(64 * 1024)
	n := 4
	data := makeData(n*int(chunkSize), 0xEE)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)
	// Set batch max to 1 chunk worth — each request should have at most 1 chunk
	cfg.DataBatchMaxBytes = chunkSize
	cfg.DataBatchMaxCount = 256 // high count so bytes limit triggers
	cfg.DataSendWorkers = 1

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	for _, req := range stream.getSent() {
		if len(req.Blocks) > 1 {
			t.Fatalf("expected at most 1 block per request (batch bytes limit), got %d", len(req.Blocks))
		}
	}
}

func TestPipeline_DataBatchMaxCount(t *testing.T) {
	chunkSize := int64(64 * 1024)
	n := 8
	data := makeData(n*int(chunkSize), 0xFF)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)
	cfg.DataBatchMaxCount = 4
	cfg.DataBatchMaxBytes = 1 << 30 // high bytes so count limit triggers
	cfg.DataSendWorkers = 1

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	for _, req := range stream.getSent() {
		if len(req.Blocks) > cfg.DataBatchMaxCount {
			t.Fatalf("expected at most %d blocks per batch, got %d", cfg.DataBatchMaxCount, len(req.Blocks))
		}
	}
}

// ---------------------------------------------------------------------------
// Stats accuracy tests
// ---------------------------------------------------------------------------

func TestPipeline_Stats(t *testing.T) {
	chunkSize := int64(64 * 1024)
	n := 4
	data := make([]byte, n*int(chunkSize))
	// 2 non-zero, 2 zero chunks
	for i := 0; i < int(chunkSize); i++ {
		data[i] = 0xAA
	}
	for i := 2 * int(chunkSize); i < 3*int(chunkSize); i++ {
		data[i] = 0xBB
	}
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	if p.Stats.ReadCount.Load() != int64(n) {
		t.Fatalf("ReadCount: expected %d, got %d", n, p.Stats.ReadCount.Load())
	}
	if p.Stats.ReadZeroCount.Load() != 2 {
		t.Fatalf("ReadZeroCount: expected 2, got %d", p.Stats.ReadZeroCount.Load())
	}
	if p.Stats.ReadBytes.Load() != int64(n)*chunkSize {
		t.Fatalf("ReadBytes: expected %d, got %d", int64(n)*chunkSize, p.Stats.ReadBytes.Load())
	}
	if p.Stats.PipelineStart.IsZero() {
		t.Fatal("PipelineStart not set")
	}
	if p.Stats.PipelineEnd.IsZero() {
		t.Fatal("PipelineEnd not set")
	}
	if p.Stats.PipelineEnd.Before(p.Stats.PipelineStart) {
		t.Fatal("PipelineEnd before PipelineStart")
	}

	summary := p.Stats.Summary()
	if summary == "" {
		t.Fatal("Stats.Summary() returned empty string")
	}
}

// ---------------------------------------------------------------------------
// Config validation tests
// ---------------------------------------------------------------------------

func TestPipeline_InvalidConfig_ChunkSizeTooLarge(t *testing.T) {
	cfg := Config{ChunkSize: 32 * 1024 * 1024} // above maxChunkSize
	reader := &testReader{}
	iter := &testIter{}
	stream := newAckStream()

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err == nil {
		t.Fatal("expected validation error for oversized ChunkSize")
	}
}

func TestPipeline_InvalidConfig_ChunkExceedsMemory(t *testing.T) {
	cfg := Config{
		ChunkSize:         1 * 1024 * 1024,
		MaxRawMemoryBytes: 512 * 1024, // less than ChunkSize
	}
	reader := &testReader{}
	iter := &testIter{}
	stream := newAckStream()

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err == nil {
		t.Fatal("expected validation error for ChunkSize > MaxRawMemoryBytes")
	}
}

func TestPipeline_InvalidConfig_ReadWorkersTooLow(t *testing.T) {
	cfg := Config{ReadWorkers: 1} // below minReadWorkers (2)
	reader := &testReader{}
	iter := &testIter{}
	stream := newAckStream()

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err == nil {
		t.Fatal("expected validation error for ReadWorkers < min")
	}
}

func TestPipeline_InvalidConfig_WindowTooLarge(t *testing.T) {
	cfg := Config{MaxWindow: 10000} // above maxMaxWindow (4096)
	reader := &testReader{}
	iter := &testIter{}
	stream := newAckStream()

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err == nil {
		t.Fatal("expected validation error for MaxWindow > max")
	}
}

// ---------------------------------------------------------------------------
// Multiple file paths
// ---------------------------------------------------------------------------

func TestPipeline_MultipleFilePaths(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := makeData(3*int(chunkSize), 0xDD)
	reader := &testReader{data: data}

	iter := &testIter{
		blocks: []ChangeBlock{
			{FilePath: "/mnt/file-a", Offset: 0, Len: chunkSize, ReqID: 0},
			{FilePath: "/mnt/file-b", Offset: 0, Len: chunkSize, ReqID: 1},
			{FilePath: "/mnt/file-c", Offset: 0, Len: chunkSize, ReqID: 2},
		},
	}

	stream := newAckStream()
	cfg := minCfg(chunkSize)

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	blocks := collectBlocks(stream.getSent())
	paths := make(map[string]bool)
	for _, b := range blocks {
		paths[b.FilePath] = true
	}
	if !paths["/mnt/file-a"] || !paths["/mnt/file-b"] || !paths["/mnt/file-c"] {
		t.Fatalf("expected all 3 file paths, got %v", paths)
	}
}

// ---------------------------------------------------------------------------
// Concurrent correctness (race detector)
// ---------------------------------------------------------------------------

func TestPipeline_Concurrent_ManyChunks(t *testing.T) {
	chunkSize := int64(64 * 1024)
	n := 64
	data := makeData(n*int(chunkSize), 0xAB)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)
	cfg.ReadWorkers = 8
	cfg.DataSendWorkers = 4

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	blocks := collectBlocks(stream.getSent())
	if len(blocks) != n {
		t.Fatalf("expected %d blocks, got %d", n, len(blocks))
	}

	// Verify all reqIDs present
	seen := make(map[uint64]bool)
	for _, b := range blocks {
		if seen[b.RequestId] {
			t.Fatalf("duplicate reqID %d", b.RequestId)
		}
		seen[b.RequestId] = true
	}
	for i := range n {
		if !seen[uint64(i)] {
			t.Fatalf("missing reqID %d", i)
		}
	}
}

// ---------------------------------------------------------------------------
// TotalSize propagation
// ---------------------------------------------------------------------------

func TestPipeline_TotalSizePropagation(t *testing.T) {
	chunkSize := int64(64 * 1024)
	data := makeData(2*int(chunkSize), 0xAA)
	reader := &testReader{data: data}

	totalSize := int64(999999)
	iter := &testIter{
		blocks: []ChangeBlock{
			{FilePath: testBlockDevice, Offset: 0, Len: chunkSize, ReqID: 0, TotalSize: totalSize},
			{FilePath: testBlockDevice, Offset: chunkSize, Len: chunkSize, ReqID: 1, TotalSize: totalSize},
		},
	}

	stream := newAckStream()
	cfg := minCfg(chunkSize)
	cfg.DataSendWorkers = 1

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	for _, b := range collectBlocks(stream.getSent()) {
		if b.TotalSize != uint64(totalSize) {
			t.Fatalf("TotalSize: expected %d, got %d", totalSize, b.TotalSize)
		}
	}
}

// ---------------------------------------------------------------------------
// Window semaphore release verification
// ---------------------------------------------------------------------------

func TestPipeline_WindowFullyReleased(t *testing.T) {
	chunkSize := int64(64 * 1024)
	n := 16
	data := makeData(n*int(chunkSize), 0xAA)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := minCfg(chunkSize)

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	// All reqIDs should be released after pipeline completes
	for i := range n {
		if !win.IsReleased(uint64(i)) {
			t.Fatalf("reqID %d not released after pipeline completion", i)
		}
	}
}

// ---------------------------------------------------------------------------
// Stress test with many workers
// ---------------------------------------------------------------------------

func TestPipeline_HighConcurrency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	chunkSize := int64(64 * 1024)
	n := 256
	data := makeData(n*int(chunkSize), 0x42)
	reader := &testReader{data: data}
	iter := &testIter{blocks: makeBlocks(n, chunkSize)}

	stream := newAckStream()
	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       16,
		DataSendWorkers:   8,
		MaxWindow:         256,
		MaxRawMemoryBytes: 256 * 1024 * 1024,
	}

	p := New(cfg)
	win := NewWindowSemaphore(cfg.MaxWindow)
	err := p.Run(context.Background(), iter, reader,
		func(_ context.Context) (grpc.BidiStreamingClient[apiv1.WriteRequest, apiv1.WriteResponse], error) {
			return stream, nil
		}, win)
	if err != nil {
		t.Fatal(err)
	}

	blocks := collectBlocks(stream.getSent())
	if len(blocks) != n {
		t.Fatalf("expected %d blocks, got %d", n, len(blocks))
	}
}
