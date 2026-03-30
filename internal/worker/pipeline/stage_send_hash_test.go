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

// mockHashStream implements bidi hash stream for tests.
// allMatch controls whether all chunks are reported as
// matched or all as mismatched.
type mockHashStream struct {
	grpc.BidiStreamingClient[
		apiv1.HashRequest,
		apiv1.HashResponse,
	]
	allMatch bool
	mu       sync.Mutex
	pending  []*apiv1.HashRequest
}

func (m *mockHashStream) Send(
	req *apiv1.HashRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending = append(m.pending, req)
	return nil
}

func (m *mockHashStream) Recv() (
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
	if !m.allMatch {
		for _, h := range req.Hashes {
			resp.MismatchedIds = append(
				resp.MismatchedIds, h.RequestId,
			)
		}
	}
	return resp, nil
}

func (m *mockHashStream) CloseSend() error {
	return nil
}

func TestStageSendHash_AllMatched(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.HashSendWorkers = 1
	cfg.HashBatchMaxCount = 4

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)
	win := NewWindowSemaphore(cfg.MaxWindow)

	data := []byte("test data here!!")
	hash := sha256.Sum256(data)

	hashedCh := make(chan HashedChunk, 2)
	for i := range uint64(2) {
		_ = mem.Acquire(ctx, cfg.ChunkSize)
		_ = win.Acquire(ctx, i)
		hashedCh <- HashedChunk{
			ReqID:  i,
			Offset: int64(i) * 16,
			Data:   data,
			Hash:   hash,
			Length: int64(len(data)),
			Held: held{
				reqID: i, memRawN: cfg.ChunkSize,
				hasWin: true, hasMem: true,
			},
		}
	}
	close(hashedCh)

	mismatchCh := make(chan HashedChunk, 2)

	factory := HashStreamFactory(func(
		_ context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.HashRequest,
		apiv1.HashResponse,
	], error) {
		return &mockHashStream{allMatch: true}, nil
	})

	err := StageSendHash(
		ctx, cfg, mem, win, factory,
		hashedCh, mismatchCh,
	)
	if err != nil {
		t.Fatal(err)
	}
	close(mismatchCh)

	count := 0
	for range mismatchCh {
		count++
	}
	if count != 0 {
		t.Fatalf("expected 0 mismatches, got %d", count)
	}
}

func TestStageSendHash_AllMismatched(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.HashSendWorkers = 1
	cfg.HashBatchMaxCount = 4

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)
	win := NewWindowSemaphore(cfg.MaxWindow)

	data := []byte("test data here!!")
	hash := sha256.Sum256(data)

	hashedCh := make(chan HashedChunk, 2)
	for i := range uint64(2) {
		_ = mem.Acquire(ctx, cfg.ChunkSize)
		_ = win.Acquire(ctx, i)
		hashedCh <- HashedChunk{
			ReqID:  i,
			Offset: int64(i) * 16,
			Data:   data,
			Hash:   hash,
			Length: int64(len(data)),
			Held: held{
				reqID: i, memRawN: cfg.ChunkSize,
				hasWin: true, hasMem: true,
			},
		}
	}
	close(hashedCh)

	mismatchCh := make(chan HashedChunk, 2)

	factory := HashStreamFactory(func(
		_ context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.HashRequest,
		apiv1.HashResponse,
	], error) {
		return &mockHashStream{allMatch: false}, nil
	})

	err := StageSendHash(
		ctx, cfg, mem, win, factory,
		hashedCh, mismatchCh,
	)
	if err != nil {
		t.Fatal(err)
	}
	close(mismatchCh)

	count := 0
	for range mismatchCh {
		count++
	}
	if count != 2 {
		t.Fatalf("expected 2 mismatches, got %d", count)
	}
}
