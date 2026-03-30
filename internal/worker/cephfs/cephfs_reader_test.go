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
	"os"
	"path/filepath"
	"sync"
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
	defer func() { _ = r.Close() }()

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
	defer func() { _ = r.Close() }()

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

func TestFileCache_RefCounting(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(
		filepath.Join(dir, "ref.bin"),
		[]byte("refcount"), 0600,
	)

	fc := NewReadCache(dir)
	defer func() { _ = fc.Close() }()

	// Two acquires increment refCount to 2.
	f1, err := fc.Acquire("ref.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := fc.Acquire("ref.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	if f1 != f2 {
		t.Fatal("expected same file handle")
	}

	// First release keeps file open.
	if err := fc.Release("ref.bin"); err != nil {
		t.Fatal(err)
	}
	// File should still be readable.
	buf := make([]byte, 3)
	if _, err := f1.ReadAt(buf, 0); err != nil {
		t.Fatal("file closed too early:", err)
	}

	// Second release closes file.
	if err := fc.Release("ref.bin"); err != nil {
		t.Fatal(err)
	}
}

func TestFileCache_SyncAndRelease(t *testing.T) {
	dir := t.TempDir()
	fc := NewWriteCache(dir)
	defer func() { _ = fc.Close() }()

	f, err := fc.Acquire("sync.bin", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := fc.SyncAndRelease("sync.bin"); err != nil {
		t.Fatal(err)
	}

	// Verify data persisted.
	got, err := os.ReadFile(filepath.Join(dir, "sync.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "data" {
		t.Errorf("expected %q, got %q", "data", got)
	}
}

func TestFileCache_ConcurrentAcquire(t *testing.T) {
	dir := t.TempDir()
	fc := NewWriteCache(dir)
	defer func() { _ = fc.Close() }()

	var wg sync.WaitGroup
	wg.Add(2)
	errs := make(chan error, 2)

	for i := range 2 {
		go func(offset int64) {
			defer wg.Done()
			f, err := fc.Acquire("concurrent.bin", 0)
			if err != nil {
				errs <- err
				return
			}
			if _, err := f.WriteAt(
				[]byte("ab"), offset,
			); err != nil {
				errs <- err
				return
			}
			errs <- fc.Release("concurrent.bin")
		}(int64(i * 2))
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	got, err := os.ReadFile(
		filepath.Join(dir, "concurrent.bin"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "abab" {
		t.Errorf("expected %q, got %q", "abab", got)
	}
}
