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
		apiv1.SyncRequest, apiv1.SyncResponse,
	]
	mu   sync.Mutex
	sent []*apiv1.SyncRequest
}

func (m *mockSyncStream) Send(
	req *apiv1.SyncRequest,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, req)
	return nil
}

func (m *mockSyncStream) Recv() (
	*apiv1.SyncResponse, error,
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

	var mu sync.Mutex
	totalSent := 0

	factory := StreamFactory(func(
		_ context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	], error) {
		return &mockSyncStream{
			sent: nil,
		}, nil
	})

	// Track sends across all workers via wrapper.
	wrappedFactory := StreamFactory(func(
		ctx context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	], error) {
		stream, err := factory(ctx)
		if err != nil {
			return nil, err
		}
		return &countingSyncStream{
			BidiStreamingClient: stream,
			mu:                  &mu,
			count:               &totalSent,
		}, nil
	})

	err := StageSendData(
		ctx, cfg, mem, win, wrappedFactory,
		inCh,
	)
	if err != nil {
		t.Fatal(err)
	}

	if totalSent == 0 {
		t.Fatal("no requests sent")
	}
}

type countingSyncStream struct {
	grpc.BidiStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	]
	mu    *sync.Mutex
	count *int
}

func (c *countingSyncStream) Send(
	req *apiv1.SyncRequest,
) error {
	c.mu.Lock()
	*c.count++
	c.mu.Unlock()
	return c.BidiStreamingClient.Send(req)
}

func (c *countingSyncStream) Recv() (
	*apiv1.SyncResponse, error,
) {
	return nil, io.EOF
}

func (c *countingSyncStream) CloseSend() error {
	return nil
}
