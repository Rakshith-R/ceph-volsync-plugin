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

type mockSyncStream struct {
	grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	]
	mu   sync.Mutex
	sent []*apiv1.WriteRequest
}

func (m *mockSyncStream) Send(
	req *apiv1.WriteRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, req)
	return nil
}

func (m *mockSyncStream) Recv() (
	*apiv1.WriteResponse, error,
) {
	return nil, io.EOF
}

func (m *mockSyncStream) CloseSend() error {
	return nil
}

func TestStageSendData_SendsAll(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)
	win := NewWindowSemaphore(cfg.MaxWindow)

	data := []byte("compressed data!")

	inCh := make(chan ReadChunk, 2)
	for i := range uint64(2) {
		_ = mem.Acquire(ctx, 16)
		_ = win.Acquire(ctx, i)
		inCh <- ReadChunk{
			ReqID:    i,
			FilePath: testBlockDevice,
			Offset:   int64(i) * 100,
			Data:     data,
			Length:   16,
			Held:     held{reqID: i, memRawN: 16, hasWin: true, hasMem: true},
		}
	}
	close(inCh)

	mock := &mockSyncStream{}

	factory := StreamFactory(func(
		_ context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	], error) {
		return mock, nil
	})

	err := StageSendData(
		ctx, cfg, &Stats{}, mem, win, factory, inCh,
	)
	if err != nil {
		t.Fatal(err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	if len(mock.sent) == 0 {
		t.Fatal("no requests sent")
	}

	for _, req := range mock.sent {
		if req.Blocks[0].FilePath != testBlockDevice {
			t.Fatalf(
				"expected FilePath /dev/block, got %s",
				req.Blocks[0].FilePath,
			)
		}
	}
}

func TestStageSendData_MultiFileBatch(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()
	// Force large batch so both chunks land in one request
	cfg.DataBatchMaxBytes = 1 << 20
	cfg.DataBatchMaxCount = 100

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)
	win := NewWindowSemaphore(cfg.MaxWindow)

	data := []byte("test")

	inCh := make(chan ReadChunk, 2)
	paths := []string{"/data/file-a", "/data/file-b"}
	for i := range uint64(2) {
		_ = mem.Acquire(ctx, 4)
		_ = win.Acquire(ctx, i)
		inCh <- ReadChunk{
			ReqID:    i,
			FilePath: paths[i],
			Offset:   0,
			Data:     data,
			Length:   4,
			Held:     held{reqID: i, memRawN: 4, hasWin: true, hasMem: true},
		}
	}
	close(inCh)

	mock := &mockSyncStream{}
	factory := StreamFactory(func(
		_ context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	], error) {
		return mock, nil
	})

	err := StageSendData(
		ctx, cfg, &Stats{}, mem, win, factory, inCh,
	)
	if err != nil {
		t.Fatal(err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	// Without path-level flushing, both blocks from different files are
	// batched into a single WriteRequest (batch limits are not reached).
	totalBlocks := 0
	for _, req := range mock.sent {
		totalBlocks += len(req.Blocks)
	}
	if totalBlocks != 2 {
		t.Fatalf("expected 2 total blocks across all requests, got %d", totalBlocks)
	}

	sentPaths := make(map[string]bool)
	for _, req := range mock.sent {
		for _, b := range req.Blocks {
			sentPaths[b.FilePath] = true
		}
	}
	if !sentPaths["/data/file-a"] || !sentPaths["/data/file-b"] {
		t.Fatal("expected blocks for both file paths")
	}
}
