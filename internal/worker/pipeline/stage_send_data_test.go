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
		for _, block := range req.Blocks {
			if block.FilePath != "/dev/block" {
				t.Fatalf(
					"expected FilePath /dev/block, got %s",
					block.FilePath,
				)
			}
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

	// With large batch limits, both chunks should be
	// in a single WriteRequest with different FilePaths.
	if len(mock.sent) != 1 {
		t.Fatalf("expected 1 request, got %d", len(mock.sent))
	}

	req := mock.sent[0]
	if len(req.Blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(req.Blocks))
	}
	if req.Blocks[0].FilePath != "/data/file-a" {
		t.Fatalf("block 0: expected /data/file-a, got %s",
			req.Blocks[0].FilePath)
	}
	if req.Blocks[1].FilePath != "/data/file-b" {
		t.Fatalf("block 1: expected /data/file-b, got %s",
			req.Blocks[1].FilePath)
	}
}
