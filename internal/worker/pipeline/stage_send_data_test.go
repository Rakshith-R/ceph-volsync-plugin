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
	"crypto/sha256"
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
	hash := sha256.Sum256(data)

	inCh := make(chan CompressedChunk, 2)
	for i := range uint64(2) {
		_ = mem.Acquire(ctx, 16)
		_ = win.Acquire(ctx, i)
		inCh <- CompressedChunk{
			ReqID:              i,
			FilePath:           "/dev/block",
			Offset:             int64(i) * 100,
			Data:               data,
			Hash:               hash,
			UncompressedLength: 16,
			Held:               held{reqID: i, memRawN: 16, hasWin: true, hasMem: true},
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
		ctx, cfg, mem, win, factory, inCh,
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
		if req.Blocks[0].FilePath != "/dev/block" {
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
	hash := sha256.Sum256(data)

	inCh := make(chan CompressedChunk, 2)
	paths := []string{"/data/file-a", "/data/file-b"}
	for i := range uint64(2) {
		_ = mem.Acquire(ctx, 4)
		_ = win.Acquire(ctx, i)
		inCh <- CompressedChunk{
			ReqID:              i,
			FilePath:           paths[i],
			Offset:             0,
			Data:               data,
			Hash:               hash,
			UncompressedLength: 4,
			Held:               held{reqID: i, memRawN: 4, hasWin: true, hasMem: true},
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
		ctx, cfg, mem, win, factory, inCh,
	)
	if err != nil {
		t.Fatal(err)
	}

	mock.mu.Lock()
	defer mock.mu.Unlock()

	// With path-level grouping, each file gets its own WriteRequest
	if len(mock.sent) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(mock.sent))
	}

	sentPaths := make(map[string]bool)
	for _, req := range mock.sent {
		sentPaths[req.Blocks[0].FilePath] = true
		if len(req.Blocks) != 1 {
			t.Fatalf("expected 1 block per request, got %d", len(req.Blocks))
		}
	}
	if !sentPaths["/data/file-a"] || !sentPaths["/data/file-b"] {
		t.Fatal("expected requests for both file paths")
	}
}
