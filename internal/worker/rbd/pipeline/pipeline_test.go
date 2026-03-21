package pipeline

import (
	"bytes"
	"context"
	"sync"
	"testing"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"google.golang.org/grpc"
)

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

type mockHashClientForPipeline struct {
	apiv1.HashServiceClient
}

func (m *mockHashClientForPipeline) CompareHashes(
	_ context.Context,
	req *apiv1.HashBatchRequest,
	_ ...grpc.CallOption,
) (*apiv1.HashBatchResponse, error) {
	resp := &apiv1.HashBatchResponse{}
	for _, h := range req.Hashes {
		resp.MismatchedIds = append(resp.MismatchedIds, h.RequestId)
	}
	return resp, nil
}

type pipelineMockStream struct {
	grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse]
	mu   sync.Mutex
	sent []*apiv1.SyncRequest
}

func (m *pipelineMockStream) Send(req *apiv1.SyncRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, req)
	return nil
}

func TestPipeline_EndToEnd(t *testing.T) {
	ctx := context.Background()

	chunkSize := int64(64 * 1024) // 64KB minimum
	data := bytes.Repeat([]byte{0xBB}, int(chunkSize*4))
	device := bytes.NewReader(data)

	iter := &mockIterator{
		blocks: []ChangeBlock{
			{FilePath: "/dev/block", Offset: 0, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize * 2, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize * 3, Len: chunkSize},
		},
	}

	stream := &pipelineMockStream{}
	hashClient := &mockHashClientForPipeline{}

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       2,
		MaxWindow:         16,
		MaxRawMemoryBytes: 2 * 1024 * 1024,
	}

	p := New(cfg)
	err := p.Run(ctx, iter, device, stream, hashClient)
	if err != nil {
		t.Fatal(err)
	}

	if len(stream.sent) == 0 {
		t.Fatal("no data sent")
	}
}

func TestPipeline_EmptyIterator(t *testing.T) {
	ctx := context.Background()

	device := bytes.NewReader(nil)
	iter := &mockIterator{}
	stream := &pipelineMockStream{}
	hashClient := &mockHashClientForPipeline{}

	cfg := Config{}
	p := New(cfg)
	err := p.Run(ctx, iter, device, stream, hashClient)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPipeline_ZeroBlocks(t *testing.T) {
	ctx := context.Background()

	chunkSize := int64(64 * 1024) // 64KB minimum
	data := make([]byte, chunkSize*2) // all zeros
	device := bytes.NewReader(data)

	iter := &mockIterator{
		blocks: []ChangeBlock{
			{FilePath: "/dev/block", Offset: 0, Len: chunkSize},
			{FilePath: "/dev/block", Offset: chunkSize, Len: chunkSize},
		},
	}

	stream := &pipelineMockStream{}
	hashClient := &mockHashClientForPipeline{}

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       2,
		MaxWindow:         16,
		MaxRawMemoryBytes: 2 * 1024 * 1024,
	}

	p := New(cfg)
	err := p.Run(ctx, iter, device, stream, hashClient)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPipeline_MultipleChunks(t *testing.T) {
	ctx := context.Background()

	chunkSize := int64(64 * 1024)
	// 8 chunks = 512KB total
	data := bytes.Repeat([]byte{0xAA, 0xBB, 0xCC, 0xDD}, int(chunkSize*8/4))
	device := bytes.NewReader(data)

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
	hashClient := &mockHashClientForPipeline{}

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       4,
		MaxWindow:         32,
		MaxRawMemoryBytes: 1024 * 1024,
	}

	p := New(cfg)
	err := p.Run(ctx, iter, device, stream, hashClient)
	if err != nil {
		t.Fatal(err)
	}

	if len(stream.sent) == 0 {
		t.Fatal("no data sent")
	}
}

func TestPipeline_ConfigValidation(t *testing.T) {
	ctx := context.Background()

	device := bytes.NewReader(nil)
	iter := &mockIterator{}
	stream := &pipelineMockStream{}
	hashClient := &mockHashClientForPipeline{}

	// Invalid ChunkSize
	cfg := Config{
		ChunkSize: 1024, // below minChunkSize (64KB)
	}

	p := New(cfg)
	err := p.Run(ctx, iter, device, stream, hashClient)
	if err == nil {
		t.Fatal("expected validation error for invalid ChunkSize")
	}
}
