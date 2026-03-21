package pipeline

import (
	"context"
	"crypto/sha256"
	"testing"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"google.golang.org/grpc"
)

type mockHashClient struct {
	apiv1.HashServiceClient
	allMatch bool
}

func (m *mockHashClient) CompareHashes(
	_ context.Context,
	req *apiv1.HashBatchRequest,
	_ ...grpc.CallOption,
) (*apiv1.HashBatchResponse, error) {
	resp := &apiv1.HashBatchResponse{}
	if !m.allMatch {
		for _, h := range req.Hashes {
			resp.MismatchedIds = append(resp.MismatchedIds, h.RequestId)
		}
	}
	return resp, nil
}

func TestStageSendHash_AllMatched(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.setDefaults()
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
			Held:   held{reqID: i, memRawN: cfg.ChunkSize, hasWin: true, hasMem: true},
		}
	}
	close(hashedCh)

	zeroCh := make(chan ZeroChunk)
	close(zeroCh)

	mismatchCh := make(chan HashedChunk, 2)
	client := &mockHashClient{allMatch: true}

	err := StageSendHash(ctx, cfg, mem, win, client, hashedCh, zeroCh, mismatchCh)
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
	cfg.setDefaults()
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
			Held:   held{reqID: i, memRawN: cfg.ChunkSize, hasWin: true, hasMem: true},
		}
	}
	close(hashedCh)

	zeroCh := make(chan ZeroChunk)
	close(zeroCh)

	mismatchCh := make(chan HashedChunk, 2)
	client := &mockHashClient{allMatch: false}

	err := StageSendHash(ctx, cfg, mem, win, client, hashedCh, zeroCh, mismatchCh)
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
