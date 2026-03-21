package pipeline

import (
	"context"
	"crypto/sha256"
	"sync"
	"testing"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"google.golang.org/grpc"
)

type mockSyncStream struct {
	grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse]
	mu   sync.Mutex
	sent []*apiv1.SyncRequest
}

func (m *mockSyncStream) Send(req *apiv1.SyncRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, req)
	return nil
}

func TestStageSendData_SendsAll(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.setDefaults()

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

	stream := &mockSyncStream{}

	err := StageSendData(ctx, cfg, mem, win, stream, inCh)
	if err != nil {
		t.Fatal(err)
	}

	if len(stream.sent) == 0 {
		t.Fatal("no requests sent")
	}
}
