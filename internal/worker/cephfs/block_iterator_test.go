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

package cephfs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

const (
	testFileA     = "a.txt"
	testFileB     = "b.txt"
	testSmallFile = "small.txt"
)

type mockFileDiffIter struct {
	blocks []changedBlock
	idx    int
	closed bool
}

func (m *mockFileDiffIter) More() bool {
	return m.idx < len(m.blocks)
}

func (m *mockFileDiffIter) Read() ([]changedBlock, error) {
	if m.idx >= len(m.blocks) {
		return nil, nil
	}
	b := []changedBlock{m.blocks[m.idx]}
	m.idx++
	return b, nil
}

func (m *mockFileDiffIter) Close() error {
	m.closed = true
	return nil
}

func TestCephFSBlockIterator_Empty(t *testing.T) {
	iter := NewCephFSBlockIterator(nil, nil)
	cb, ok := iter.Next()
	if ok || cb != nil {
		t.Fatal("expected empty iterator")
	}
	_ = iter.Close()
}

func TestCephFSBlockIterator_TwoFiles(t *testing.T) {
	fileA := &mockFileDiffIter{
		blocks: []changedBlock{
			{Offset: 0, Len: 4096},
			{Offset: 4096, Len: 4096},
		},
	}
	fileB := &mockFileDiffIter{
		blocks: []changedBlock{
			{Offset: 0, Len: 8192},
		},
	}
	iters := map[string]*mockFileDiffIter{
		testFileA: fileA, testFileB: fileB,
	}

	newIter := func(path string) (
		fileDiffIterator, error,
	) {
		return iters[path], nil
	}

	iter := NewCephFSBlockIterator(
		newIter, []string{testFileA, testFileB},
	)

	var got []*pipeline.ChangeBlock
	for {
		cb, ok := iter.Next()
		if !ok {
			break
		}
		got = append(got, cb)
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(got))
	}
	if got[0].FilePath != testFileA {
		t.Errorf("expected a.txt, got %s", got[0].FilePath)
	}
	if got[2].FilePath != testFileB {
		t.Errorf("expected b.txt, got %s", got[2].FilePath)
	}
	_ = iter.Close()
	if !fileA.closed {
		t.Error("fileA iterator not closed")
	}
}

func TestCephFSBlockIterator_FromChan(t *testing.T) {
	fileA := &mockFileDiffIter{
		blocks: []changedBlock{
			{Offset: 0, Len: 4096},
		},
	}
	fileB := &mockFileDiffIter{
		blocks: []changedBlock{
			{Offset: 0, Len: 8192},
			{Offset: 8192, Len: 4096},
		},
	}
	iters := map[string]*mockFileDiffIter{
		testFileA: fileA, testFileB: fileB,
	}

	newIter := func(path string) (
		fileDiffIterator, error,
	) {
		return iters[path], nil
	}

	fileCh := make(chan changedFileEntry, 2)
	fileCh <- changedFileEntry{path: testFileA, size: -1}
	fileCh <- changedFileEntry{path: testFileB, size: -1}
	close(fileCh)

	boundaryCh := make(chan fileBoundary, 10)
	ctx := context.Background()

	iter := NewCephFSBlockIterator(
		newIter, nil,
		WithFileChan(fileCh),
		WithContext(ctx),
		WithBoundaryChan(boundaryCh),
	)

	var got []*pipeline.ChangeBlock
	for {
		cb, ok := iter.Next()
		if !ok {
			break
		}
		got = append(got, cb)
	}
	_ = iter.Close()

	if len(got) != 3 {
		t.Fatalf("expected 3 blocks, got %d", len(got))
	}
	if got[0].FilePath != testFileA {
		t.Errorf("block 0: expected a.txt, got %s",
			got[0].FilePath)
	}
	if got[0].ReqID != 0 {
		t.Errorf("block 0: expected reqID 0, got %d",
			got[0].ReqID)
	}
	if got[1].FilePath != testFileB {
		t.Errorf("block 1: expected b.txt, got %s",
			got[1].FilePath)
	}
	if got[1].ReqID != 1 {
		t.Errorf("block 1: expected reqID 1, got %d",
			got[1].ReqID)
	}
	if got[2].ReqID != 2 {
		t.Errorf("block 2: expected reqID 2, got %d",
			got[2].ReqID)
	}

	// Verify boundaries emitted.
	close(boundaryCh)
	var boundaries []fileBoundary
	for fb := range boundaryCh {
		boundaries = append(boundaries, fb)
	}
	if len(boundaries) != 2 {
		t.Fatalf(
			"expected 2 boundaries, got %d",
			len(boundaries),
		)
	}
	if boundaries[0].path != testFileA {
		t.Errorf("boundary 0: expected a.txt, got %s",
			boundaries[0].path)
	}
	if boundaries[0].lastReqID != 0 {
		t.Errorf(
			"boundary 0: expected lastReqID 0, got %d",
			boundaries[0].lastReqID,
		)
	}
	if boundaries[1].path != testFileB {
		t.Errorf("boundary 1: expected b.txt, got %s",
			boundaries[1].path)
	}
	if boundaries[1].lastReqID != 2 {
		t.Errorf(
			"boundary 1: expected lastReqID 2, got %d",
			boundaries[1].lastReqID,
		)
	}
}

func TestCephFSBlockIterator_FromChan_Empty(
	t *testing.T,
) {
	fileCh := make(chan changedFileEntry)
	close(fileCh)

	iter := NewCephFSBlockIterator(
		nil, nil,
		WithFileChan(fileCh),
	)

	cb, ok := iter.Next()
	if ok || cb != nil {
		t.Fatal("expected empty iterator")
	}
	_ = iter.Close()
}

func TestCephFSBlockIterator_FromChan_CtxCancel(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	fileCh := make(chan changedFileEntry) // unbuffered, will block

	iter := NewCephFSBlockIterator(
		nil, nil,
		WithFileChan(fileCh),
		WithContext(ctx),
	)

	done := make(chan struct{})
	go func() {
		cb, ok := iter.Next()
		if ok || cb != nil {
			t.Error("expected cancelled iterator")
		}
		close(done)
	}()

	// Give goroutine time to block on fileCh.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
		// Success — Next() returned after cancel.
	case <-time.After(2 * time.Second):
		t.Fatal("Next() did not unblock after cancel")
	}
	_ = iter.Close()
}

func TestCephFSBlockIterator_SmallFileSynthetic(t *testing.T) {
	iterCalled := false
	newIter := func(path string) (fileDiffIterator, error) {
		iterCalled = true
		return nil, fmt.Errorf("should not be called for small files")
	}

	ch := make(chan changedFileEntry, 2)
	boundaryCh := make(chan fileBoundary, 2)
	ctx := context.Background()

	// 1MB file, threshold 12MB — should be synthetic.
	ch <- changedFileEntry{path: testSmallFile, size: 1024 * 1024}
	close(ch)

	it := NewCephFSBlockIterator(newIter, nil,
		WithFileChan(ch),
		WithContext(ctx),
		WithBoundaryChan(boundaryCh),
		WithSmallFileThreshold(12*1024*1024),
	)

	block, ok := it.Next()
	if !ok {
		t.Fatal("expected a block from small file")
	}
	if block.FilePath != testSmallFile {
		t.Errorf("expected small.txt, got %s", block.FilePath)
	}
	if block.Offset != 0 {
		t.Errorf("expected offset 0, got %d", block.Offset)
	}
	if block.Len != 1024*1024 {
		t.Errorf("expected len %d, got %d", 1024*1024, block.Len)
	}
	if block.TotalSize != 1024*1024 {
		t.Errorf("expected totalSize %d, got %d", 1024*1024, block.TotalSize)
	}

	_, ok = it.Next()
	if ok {
		t.Error("expected no more blocks after small file")
	}

	if iterCalled {
		t.Error("newIter should not be called for small files")
	}

	// Check boundary was emitted.
	_ = it.Close()
	close(boundaryCh)
	var boundaries []fileBoundary
	for fb := range boundaryCh {
		boundaries = append(boundaries, fb)
	}
	if len(boundaries) != 1 {
		t.Fatalf("expected 1 boundary, got %d", len(boundaries))
	}
	fb := boundaries[0]
	if fb.path != testSmallFile {
		t.Errorf("boundary path: expected small.txt, got %s", fb.path)
	}
	if fb.lastReqID != 0 {
		t.Errorf("boundary lastReqID: expected 0, got %d", fb.lastReqID)
	}
	if fb.totalSize != 1024*1024 {
		t.Errorf("boundary totalSize: expected %d, got %d", 1024*1024, fb.totalSize)
	}
}

func TestCephFSBlockIterator_BoundaryOverflow(t *testing.T) {
	// Verify that when boundaryCh is smaller than the
	// number of files, boundaries are buffered internally
	// and flushed on Close() — no deadlock.
	fileCount := 10
	ch := make(chan changedFileEntry, fileCount)
	boundaryCh := make(chan fileBoundary, 2) // deliberately small
	ctx := context.Background()

	newIter := func(path string) (fileDiffIterator, error) {
		return nil, fmt.Errorf("should not be called")
	}

	for i := range fileCount {
		ch <- changedFileEntry{path: fmt.Sprintf("f%d.txt", i), size: 1024}
	}
	close(ch)

	it := NewCephFSBlockIterator(newIter, nil,
		WithFileChan(ch),
		WithContext(ctx),
		WithBoundaryChan(boundaryCh),
		WithSmallFileThreshold(12*1024*1024),
	)

	// Drain all blocks.
	count := 0
	for {
		_, ok := it.Next()
		if !ok {
			break
		}
		count++
		// Drain boundaryCh to simulate the commitDrainer.
		for len(boundaryCh) > 0 {
			<-boundaryCh
		}
	}
	if count != fileCount {
		t.Fatalf("expected %d blocks, got %d", fileCount, count)
	}

	_ = it.Close()

	// Drain remaining boundaries after Close flushed them.
	for len(boundaryCh) > 0 {
		<-boundaryCh
	}
}

func TestCephFSBlockIterator_EmptyFileSkipped(t *testing.T) {
	newIter := func(path string) (fileDiffIterator, error) {
		return nil, fmt.Errorf("should not be called for empty files")
	}

	ch := make(chan changedFileEntry, 2)
	ctx := context.Background()

	ch <- changedFileEntry{path: "empty.txt", size: 0}
	close(ch)

	it := NewCephFSBlockIterator(newIter, nil,
		WithFileChan(ch),
		WithContext(ctx),
		WithSmallFileThreshold(12*1024*1024),
	)

	_, ok := it.Next()
	if ok {
		t.Error("empty file should produce no blocks")
	}
	_ = it.Close()
}
