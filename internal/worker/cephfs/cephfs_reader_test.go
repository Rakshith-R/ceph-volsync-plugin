package cephfs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCephFSReader_ReadAt(t *testing.T) {
	dir := t.TempDir()
	content := []byte("hello world data block!")
	path := filepath.Join(dir, "testfile.bin")
	_ = os.WriteFile(path, content, 0600)

	r := newCephFSReader(dir)

	data, err := r.ReadAt("testfile.bin", 0, int64(len(content)))
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != string(content) {
		t.Errorf("expected %q, got %q", content, data)
	}

	_ = r.CloseFile("testfile.bin")
	_ = r.Close()
}

func TestCephFSReader_PartialRead(t *testing.T) {
	dir := t.TempDir()
	content := []byte("abcdefghij")
	_ = os.WriteFile(filepath.Join(dir, "f.bin"), content, 0600)

	r := newCephFSReader(dir)
	defer r.Close()

	data, err := r.ReadAt("f.bin", 3, 4)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "defg" {
		t.Errorf("expected %q, got %q", "defg", data)
	}
}

func TestCephFSReader_CloseFile(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "a.bin"), []byte("aaa"), 0600)
	_ = os.WriteFile(filepath.Join(dir, "b.bin"), []byte("bbb"), 0600)

	r := newCephFSReader(dir)
	defer r.Close()

	_, _ = r.ReadAt("a.bin", 0, 3)
	_ = r.CloseFile("a.bin")

	// After close, reading a different file works.
	data, err := r.ReadAt("b.bin", 0, 3)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "bbb" {
		t.Errorf("expected %q, got %q", "bbb", data)
	}
}
