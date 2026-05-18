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

package rbd

import (
	"testing"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

type mockDiffIterator struct {
	blocks []diffBlock
	idx    int
}

func (m *mockDiffIterator) Next() (*diffBlock, bool) {
	if m.idx >= len(m.blocks) {
		return nil, false
	}
	b := &m.blocks[m.idx]
	m.idx++
	return b, true
}

func (m *mockDiffIterator) Close() error { return nil }

func collectAll(a *rbdIterAdapter) []*pipeline.ChangeBlock {
	var result []*pipeline.ChangeBlock
	for {
		b, ok := a.Next()
		if !ok {
			break
		}
		result = append(result, b)
	}
	return result
}

func TestRbdIterAdapter_FullDiff_GapBefore(t *testing.T) {
	iter := &mockDiffIterator{
		blocks: []diffBlock{
			{Offset: 1024, Len: 512},
		},
	}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  2048,
		chunkSize:  2048,
		isFullDiff: true,
	}

	blocks := collectAll(adapter)
	if len(blocks) != 3 {
		t.Fatalf("expected 3 blocks (gap + data + trailing), got %d", len(blocks))
	}
	// Gap before data
	if !blocks[0].IsZero || blocks[0].Offset != 0 || blocks[0].Len != 1024 {
		t.Errorf("block 0: expected zero gap [0,1024), got offset=%d len=%d isZero=%v", blocks[0].Offset, blocks[0].Len, blocks[0].IsZero)
	}
	// Data extent
	if blocks[1].IsZero || blocks[1].Offset != 1024 || blocks[1].Len != 512 {
		t.Errorf("block 1: expected data [1024,1536), got offset=%d len=%d isZero=%v", blocks[1].Offset, blocks[1].Len, blocks[1].IsZero)
	}
	// Trailing zero
	if !blocks[2].IsZero || blocks[2].Offset != 1536 || blocks[2].Len != 512 {
		t.Errorf("block 2: expected trailing zero [1536,2048), got offset=%d len=%d isZero=%v", blocks[2].Offset, blocks[2].Len, blocks[2].IsZero)
	}
}

func TestRbdIterAdapter_FullDiff_GapBetween(t *testing.T) {
	iter := &mockDiffIterator{
		blocks: []diffBlock{
			{Offset: 0, Len: 256},
			{Offset: 512, Len: 256},
		},
	}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  768,
		chunkSize:  1024,
		isFullDiff: true,
	}

	blocks := collectAll(adapter)
	if len(blocks) != 3 {
		t.Fatalf("expected 3 blocks (data + gap + data), got %d", len(blocks))
	}
	if blocks[0].IsZero || blocks[0].Offset != 0 || blocks[0].Len != 256 {
		t.Errorf("block 0: expected data [0,256)")
	}
	if !blocks[1].IsZero || blocks[1].Offset != 256 || blocks[1].Len != 256 {
		t.Errorf("block 1: expected zero gap [256,512), got offset=%d len=%d isZero=%v", blocks[1].Offset, blocks[1].Len, blocks[1].IsZero)
	}
	if blocks[2].IsZero || blocks[2].Offset != 512 || blocks[2].Len != 256 {
		t.Errorf("block 2: expected data [512,768)")
	}
}

func TestRbdIterAdapter_FullDiff_TrailingZero(t *testing.T) {
	iter := &mockDiffIterator{
		blocks: []diffBlock{
			{Offset: 0, Len: 512},
		},
	}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  1024,
		chunkSize:  1024,
		isFullDiff: true,
	}

	blocks := collectAll(adapter)
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks (data + trailing zero), got %d", len(blocks))
	}
	if blocks[0].IsZero || blocks[0].Offset != 0 || blocks[0].Len != 512 {
		t.Errorf("block 0: expected data [0,512)")
	}
	if !blocks[1].IsZero || blocks[1].Offset != 512 || blocks[1].Len != 512 {
		t.Errorf("block 1: expected trailing zero [512,1024)")
	}
}

func TestRbdIterAdapter_FullDiff_NoGap(t *testing.T) {
	iter := &mockDiffIterator{
		blocks: []diffBlock{
			{Offset: 0, Len: 512},
			{Offset: 512, Len: 512},
		},
	}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  1024,
		chunkSize:  1024,
		isFullDiff: true,
	}

	blocks := collectAll(adapter)
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks (contiguous data), got %d", len(blocks))
	}
	for i, b := range blocks {
		if b.IsZero {
			t.Errorf("block %d: should not be zero", i)
		}
	}
}

func TestRbdIterAdapter_IncrementalDiff_NoGaps(t *testing.T) {
	iter := &mockDiffIterator{
		blocks: []diffBlock{
			{Offset: 1024, Len: 512},
		},
	}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  4096,
		chunkSize:  4096,
		isFullDiff: false,
	}

	blocks := collectAll(adapter)
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block (no gaps for incremental), got %d", len(blocks))
	}
	if blocks[0].IsZero {
		t.Error("block should not be zero for incremental diff")
	}
}

func TestRbdIterAdapter_FullDiff_LargeGapChunked(t *testing.T) {
	chunkSize := int64(256)
	iter := &mockDiffIterator{
		blocks: []diffBlock{
			{Offset: 1024, Len: 256},
		},
	}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  1280,
		chunkSize:  chunkSize,
		isFullDiff: true,
	}

	blocks := collectAll(adapter)
	// Gap [0,1024) chunked into 4 × 256 + data [1024,1280)
	if len(blocks) != 5 {
		t.Fatalf("expected 5 blocks (4 zero chunks + 1 data), got %d", len(blocks))
	}
	for i := range 4 {
		if !blocks[i].IsZero || blocks[i].Len != chunkSize {
			t.Errorf("block %d: expected zero chunk of %d, got isZero=%v len=%d", i, chunkSize, blocks[i].IsZero, blocks[i].Len)
		}
		if blocks[i].Offset != int64(i)*chunkSize {
			t.Errorf("block %d: expected offset %d, got %d", i, int64(i)*chunkSize, blocks[i].Offset)
		}
	}
	if blocks[4].IsZero || blocks[4].Offset != 1024 {
		t.Errorf("block 4: expected data at 1024, got offset=%d isZero=%v", blocks[4].Offset, blocks[4].IsZero)
	}
}

func TestRbdIterAdapter_FilePath(t *testing.T) {
	iter := &mockDiffIterator{
		blocks: []diffBlock{{Offset: 0, Len: 64}},
	}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  64,
		chunkSize:  1024,
		isFullDiff: false,
	}
	blocks := collectAll(adapter)
	if len(blocks) != 1 || blocks[0].FilePath != constant.DevicePath {
		t.Errorf("expected FilePath=%s, got %s", constant.DevicePath, blocks[0].FilePath)
	}
}

func TestRbdIterAdapter_ReqIDSequential(t *testing.T) {
	iter := &mockDiffIterator{
		blocks: []diffBlock{
			{Offset: 0, Len: 256},
			{Offset: 512, Len: 256},
		},
	}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  1024,
		chunkSize:  1024,
		isFullDiff: true,
	}

	blocks := collectAll(adapter)
	for i, b := range blocks {
		if b.ReqID != uint64(i) {
			t.Errorf("block %d: expected ReqID=%d, got %d", i, i, b.ReqID)
		}
	}
}

func TestRbdIterAdapter_EmptyIterFullDiff(t *testing.T) {
	iter := &mockDiffIterator{}
	adapter := &rbdIterAdapter{
		iter:       iter,
		totalSize:  512,
		chunkSize:  512,
		isFullDiff: true,
	}

	blocks := collectAll(adapter)
	if len(blocks) != 1 {
		t.Fatalf("expected 1 zero block for empty iter with full diff, got %d", len(blocks))
	}
	if !blocks[0].IsZero || blocks[0].Offset != 0 || blocks[0].Len != 512 {
		t.Errorf("expected full zero block [0,512), got offset=%d len=%d isZero=%v", blocks[0].Offset, blocks[0].Len, blocks[0].IsZero)
	}
}
