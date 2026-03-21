package cephfs

import (
	"testing"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
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
		"a.txt": fileA, "b.txt": fileB,
	}

	newIter := func(path string) (
		fileDiffIterator, error,
	) {
		return iters[path], nil
	}

	iter := NewCephFSBlockIterator(
		newIter, []string{"a.txt", "b.txt"},
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
	if got[0].FilePath != "a.txt" {
		t.Errorf("expected a.txt, got %s", got[0].FilePath)
	}
	if got[2].FilePath != "b.txt" {
		t.Errorf("expected b.txt, got %s", got[2].FilePath)
	}
	_ = iter.Close()
	if !fileA.closed {
		t.Error("fileA iterator not closed")
	}
}
