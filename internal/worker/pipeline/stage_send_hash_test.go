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
		apiv1.HashBatchRequest,
		apiv1.HashBatchResponse,
	]
	allMatch bool
	mu       sync.Mutex
	pending  []*apiv1.HashBatchRequest
}

func (m *mockHashStream) Send(
	req *apiv1.HashBatchRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pending = append(m.pending, req)
	return nil
}

func (m *mockHashStream) Recv() (
	*apiv1.HashBatchResponse, error,
) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.pending) == 0 {
		return nil, io.EOF
	}
	req := m.pending[0]
	m.pending = m.pending[1:]
	resp := &apiv1.HashBatchResponse{}
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

	zeroCh := make(chan ZeroChunk)
	close(zeroCh)

	mismatchCh := make(chan HashedChunk, 2)

	factory := HashStreamFactory(func(
		_ context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.HashBatchRequest,
		apiv1.HashBatchResponse,
	], error) {
		return &mockHashStream{allMatch: true}, nil
	})

	err := StageSendHash(
		ctx, cfg, mem, win, factory,
		hashedCh, zeroCh, mismatchCh,
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

	zeroCh := make(chan ZeroChunk)
	close(zeroCh)

	mismatchCh := make(chan HashedChunk, 2)

	factory := HashStreamFactory(func(
		_ context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.HashBatchRequest,
		apiv1.HashBatchResponse,
	], error) {
		return &mockHashStream{allMatch: false}, nil
	})

	err := StageSendHash(
		ctx, cfg, mem, win, factory,
		hashedCh, zeroCh, mismatchCh,
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
