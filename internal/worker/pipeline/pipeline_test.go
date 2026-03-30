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
	"io"
	"sync"
	"testing"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"google.golang.org/grpc"
)

type mockDataReaderForPipeline struct {
	data []byte
}

func (m *mockDataReaderForPipeline) ReadAt(
	_ string, offset, length int64,
) ([]byte, error) {
	end := offset + length
	if end > int64(len(m.data)) {
		end = int64(len(m.data))
	}
	return append([]byte(nil), m.data[offset:end]...), nil
}

func (m *mockDataReaderForPipeline) CloseFile(_ string) error {
	return nil
}

type mockIterator struct {
	blocks []ChangeBlock
	idx    int
}

func (m *mockIterator) Next() (*ChangeBlock, bool) {
	if m.idx >= len(m.blocks) {
		return nil, false
	}
	b := &m.blocks[m.idx]
	m.idx++
	return b, true
}

func (m *mockIterator) Close() error { return nil }

type pipelineMockStream struct {
	grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	]
	mu   sync.Mutex
	sent []*apiv1.WriteRequest
}

func (m *pipelineMockStream) Send(
	req *apiv1.WriteRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, req)
	return nil
}

func (m *pipelineMockStream) Recv() (
	*apiv1.WriteResponse, error,
) {
	return nil, io.EOF
}

func (m *pipelineMockStream) CloseSend() error {
	return nil
}

// allMismatchHashStream returns all request IDs
// as mismatched, forcing the full pipeline path.
type allMismatchHashStream struct {
	grpc.BidiStreamingClient[
		apiv1.HashRequest,
		apiv1.HashResponse,
	]
	mu      sync.Mutex
	pending []*apiv1.HashRequest
}

func (m *allMismatchHashStream) Send(
	req *apiv1.HashRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending = append(m.pending, req)
	return nil
}

func (m *allMismatchHashStream) Recv() (
	*apiv1.HashResponse, error,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) == 0 {
		return nil, io.EOF
	}
	req := m.pending[0]
	m.pending = m.pending[1:]
	resp := &apiv1.HashResponse{}
	for _, h := range req.Hashes {
		resp.MismatchedIds = append(
			resp.MismatchedIds, h.RequestId,
		)
	}
	return resp, nil
}

func (m *allMismatchHashStream) CloseSend() error {
	return nil
}

func newStreamFactory(
	stream *pipelineMockStream,
) StreamFactory {
	return func(_ context.Context) (
		grpc.BidiStreamingClient[
			apiv1.WriteRequest, apiv1.WriteResponse,
		], error,
	) {
		return stream, nil
	}
}

func newHashStreamFactory() HashStreamFactory {
	return func(_ context.Context) (
		grpc.BidiStreamingClient[
			apiv1.HashRequest,
			apiv1.HashResponse,
		], error,
	) {
		return &allMismatchHashStream{}, nil
	}
}

func TestPipeline_EndToEnd(t *testing.T) {
	ctx := context.Background()

	chunkSize := int64(64 * 1024) // 64KB minimum
	data := make([]byte, chunkSize*4)
	for i := range data {
		data[i] = 0xBB
	}
	reader := &mockDataReaderForPipeline{data: data}

	iter := &mockIterator{
		blocks: []ChangeBlock{
			{FilePath: "/dev/block", Offset: 0, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize * 2, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize * 3, Len: chunkSize},
		},
	}

	stream := &pipelineMockStream{}

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       2,
		MaxWindow:         16,
		MaxRawMemoryBytes: 2 * 1024 * 1024,
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(
		ctx, iter, reader,
		newStreamFactory(stream),
		newHashStreamFactory(),
		win,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(stream.sent) == 0 {
		t.Fatal("no data sent")
	}
}

func TestPipeline_EmptyIterator(t *testing.T) {
	ctx := context.Background()

	reader := &mockDataReaderForPipeline{data: nil}
	iter := &mockIterator{}
	stream := &pipelineMockStream{}

	cfg := Config{}
	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(
		ctx, iter, reader,
		newStreamFactory(stream),
		newHashStreamFactory(),
		win,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPipeline_ZeroBlocks(t *testing.T) {
	ctx := context.Background()

	chunkSize := int64(64 * 1024)     // 64KB minimum
	data := make([]byte, chunkSize*2) // all zeros
	reader := &mockDataReaderForPipeline{data: data}

	iter := &mockIterator{
		blocks: []ChangeBlock{
			{FilePath: "/dev/block", Offset: 0, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize, Len: chunkSize},
		},
	}

	stream := &pipelineMockStream{}

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       2,
		MaxWindow:         16,
		MaxRawMemoryBytes: 2 * 1024 * 1024,
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(
		ctx, iter, reader,
		newStreamFactory(stream),
		newHashStreamFactory(),
		win,
	)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPipeline_MultipleChunks(t *testing.T) {
	ctx := context.Background()

	chunkSize := int64(64 * 1024)
	// 8 chunks = 512KB total
	totalSize := chunkSize * 8
	data := make([]byte, totalSize)
	pattern := []byte{0xAA, 0xBB, 0xCC, 0xDD}
	for i := range data {
		data[i] = pattern[i%len(pattern)]
	}
	reader := &mockDataReaderForPipeline{data: data}

	var blocks []ChangeBlock
	for offset := int64(0); offset < int64(len(data)); offset += chunkSize {
		length := chunkSize
		if offset+length > int64(len(data)) {
			length = int64(len(data)) - offset
		}
		blocks = append(blocks, ChangeBlock{
			FilePath: "/dev/block",
			Offset:   offset,
			Len:      length,
		})
	}

	iter := &mockIterator{blocks: blocks}
	stream := &pipelineMockStream{}

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       4,
		MaxWindow:         32,
		MaxRawMemoryBytes: 1024 * 1024,
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(
		ctx, iter, reader,
		newStreamFactory(stream),
		newHashStreamFactory(),
		win,
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(stream.sent) == 0 {
		t.Fatal("no data sent")
	}
}

func TestPipeline_NilHashStream(t *testing.T) {
	ctx := context.Background()

	chunkSize := int64(64 * 1024)
	data := make([]byte, chunkSize*4)
	for i := range data {
		data[i] = 0xCC
	}
	reader := &mockDataReaderForPipeline{data: data}

	iter := &mockIterator{
		blocks: []ChangeBlock{
			{FilePath: "/dev/block", Offset: 0, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize * 2, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize * 3, Len: chunkSize},
		},
	}

	stream := &pipelineMockStream{}
	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       2,
		MaxWindow:         16,
		MaxRawMemoryBytes: 2 * 1024 * 1024,
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(
		ctx, iter, reader,
		newStreamFactory(stream), nil,
		win,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(stream.sent) == 0 {
		t.Fatal("expected data to be sent with nil hash stream")
	}
}

func TestPipeline_ConfigValidation(t *testing.T) {
	ctx := context.Background()

	reader := &mockDataReaderForPipeline{data: nil}
	iter := &mockIterator{}
	stream := &pipelineMockStream{}

	// Invalid ChunkSize
	cfg := Config{
		ChunkSize: 1024, // below minChunkSize (64KB)
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(
		ctx, iter, reader,
		newStreamFactory(stream),
		newHashStreamFactory(),
		win,
	)
	if err == nil {
		t.Fatal("expected validation error for invalid ChunkSize")
	}
}
