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
	"testing"
	"time"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

const (
	testFileA = "a.txt"
	testFileB = "b.txt"
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

	fileCh := make(chan string, 2)
	fileCh <- testFileA
	fileCh <- testFileB
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
	fileCh := make(chan string)
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

	fileCh := make(chan string) // unbuffered, will block

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
