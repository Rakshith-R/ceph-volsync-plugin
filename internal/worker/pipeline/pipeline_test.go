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
	"testing"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"google.golang.org/grpc"
)

const testBlockDevice = "/dev/block"

type mockDataReaderForPipeline struct {
	data []byte
}

func (m *mockDataReaderForPipeline) ReadAt(
	_ string, offset, length int64,
) ([]byte, error) {
	end := min(offset+length, int64(len(m.data)))
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

func newStreamFactory(
	stream *ackStream,
) StreamFactory {
	return func(_ context.Context) (
		grpc.BidiStreamingClient[
			apiv1.WriteRequest, apiv1.WriteResponse,
		], error,
	) {
		return stream, nil
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
			{FilePath: testBlockDevice, Offset: 0, Len: chunkSize},
			{FilePath: testBlockDevice, Offset: chunkSize, Len: chunkSize},
			{FilePath: testBlockDevice, Offset: chunkSize * 2, Len: chunkSize},
			{FilePath: testBlockDevice, Offset: chunkSize * 3, Len: chunkSize},
		},
	}

	stream := newAckStream()

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       2,
		MaxWindow:         16,
		MaxRawMemoryBytes: 2 * 1024 * 1024,
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(ctx, iter, reader, newStreamFactory(stream), win)
	if err != nil {
		t.Fatal(err)
	}

	if len(stream.getSent()) == 0 {
		t.Fatal("no data sent")
	}
}

func TestPipeline_EmptyIterator(t *testing.T) {
	ctx := context.Background()

	reader := &mockDataReaderForPipeline{data: nil}
	iter := &mockIterator{}
	stream := newAckStream()

	cfg := Config{}
	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(ctx, iter, reader, newStreamFactory(stream), win)
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
			{FilePath: testBlockDevice, Offset: 0, Len: chunkSize},
			{FilePath: testBlockDevice, Offset: chunkSize, Len: chunkSize},
		},
	}

	stream := newAckStream()

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       2,
		MaxWindow:         16,
		MaxRawMemoryBytes: 2 * 1024 * 1024,
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(ctx, iter, reader, newStreamFactory(stream), win)
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
			FilePath: testBlockDevice,
			Offset:   offset,
			Len:      length,
		})
	}

	iter := &mockIterator{blocks: blocks}
	stream := newAckStream()

	cfg := Config{
		ChunkSize:         chunkSize,
		ReadWorkers:       4,
		MaxWindow:         32,
		MaxRawMemoryBytes: 1024 * 1024,
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(ctx, iter, reader, newStreamFactory(stream), win)
	if err != nil {
		t.Fatal(err)
	}

	if len(stream.getSent()) == 0 {
		t.Fatal("no data sent")
	}
}

func TestPipeline_ConfigValidation(t *testing.T) {
	ctx := context.Background()

	reader := &mockDataReaderForPipeline{data: nil}
	iter := &mockIterator{}
	stream := newAckStream()

	// Invalid ChunkSize
	cfg := Config{
		ChunkSize: 1024, // below minChunkSize (64KB)
	}

	p := New(cfg)
	win := NewWindowSemaphore(64)
	err := p.Run(ctx, iter, reader, newStreamFactory(stream), win)
	if err == nil {
		t.Fatal("expected validation error for invalid ChunkSize")
	}
}
