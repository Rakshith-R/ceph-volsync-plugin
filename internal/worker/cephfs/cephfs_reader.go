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
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

type fileEntry struct {
	file     *os.File
	refCount int
}

const numCacheShards = 16

type fileCacheShard struct {
	mu    sync.Mutex
	files map[string]*fileEntry
}

// FileCache is a ref-counted file handle cache.
// Thread-safe for concurrent use across goroutines.
// A single instance is shared by all components
// that access files under the same baseDir.
// Uses sharded locking to reduce contention when
// many goroutines open different files concurrently.
type FileCache struct {
	baseDir string
	mode    int
	perm    os.FileMode
	shards  [numCacheShards]fileCacheShard
}

// shard returns the shard responsible for key.
func (fc *FileCache) shard(key string) *fileCacheShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &fc.shards[h.Sum32()%numCacheShards]
}

// NewFileCache creates a FileCache rooted at
// baseDir. mode and perm control os.OpenFile flags.
func NewFileCache(
	baseDir string, mode int, perm os.FileMode,
) *FileCache {
	fc := &FileCache{baseDir: baseDir, mode: mode, perm: perm}
	for i := range fc.shards {
		fc.shards[i].files = make(map[string]*fileEntry)
	}
	return fc
}

// NewReadCache creates a read-only FileCache
// rooted at baseDir.
func NewReadCache(baseDir string) *FileCache {
	return NewFileCache(baseDir, os.O_RDONLY, 0)
}

// NewWriteCache creates a read-write FileCache
// rooted at baseDir with file creation enabled.
func NewWriteCache(baseDir string) *FileCache {
	return NewFileCache(
		baseDir,
		os.O_RDWR|os.O_CREATE, 0644, //nolint:gosec // G301: rsync daemon needs world-readable dirs
	)
}

// Acquire returns the file for relPath, opening
// it if needed, and increments refCount.
// If totalSize > 0 and the file is smaller, it is
// truncated to totalSize on first open. Pass 0 for
// read-only access (no truncation, no stat overhead).
func (fc *FileCache) Acquire(
	relPath string, totalSize int64,
) (*os.File, error) {
	clean := filepath.Clean(relPath)
	if strings.Contains(clean, "..") {
		return nil, fmt.Errorf(
			"invalid path: path traversal not allowed: %s",
			relPath,
		)
	}

	s := fc.shard(relPath)
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.files[relPath]; ok {
		entry.refCount++
		return entry.file, nil
	}

	full := filepath.Join(fc.baseDir, relPath)
	dir := filepath.Dir(full)
	if fc.mode&os.O_CREATE != 0 {
		if err := os.MkdirAll(dir, 0755); err != nil { //nolint:gosec // G301
			return nil, fmt.Errorf(
				"mkdir %s: %w", dir, err,
			)
		}
	}

	f, err := os.OpenFile( //nolint:gosec // G304: path constructed from validated input
		full, fc.mode, fc.perm,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"open %s: %w", full, err,
		)
	}

	if totalSize > 0 {
		fi, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf(
				"stat %s: %w", full, err,
			)
		}
		if fi.Size() < totalSize {
			if err := f.Truncate(totalSize); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf(
					"truncate %s to %d: %w",
					full, totalSize, err,
				)
			}
		}
	}

	s.files[relPath] = &fileEntry{
		file: f, refCount: 1,
	}
	return f, nil
}

// Release decrements refCount for relPath.
// Closes the file when refCount reaches 0.
func (fc *FileCache) Release(
	relPath string,
) error {
	s := fc.shard(relPath)
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, ok := s.files[relPath]
	if !ok {
		return nil
	}
	entry.refCount--
	if entry.refCount <= 0 {
		delete(s.files, relPath)
		return entry.file.Close()
	}
	return nil
}

// SyncAndRelease syncs the file then releases
// one reference. For destination write commits.
func (fc *FileCache) SyncAndRelease(
	relPath string,
) error {
	s := fc.shard(relPath)
	s.mu.Lock()
	entry, ok := s.files[relPath]
	if !ok {
		s.mu.Unlock()
		return nil
	}
	entry.refCount++
	s.mu.Unlock()

	syncErr := entry.file.Sync()

	// Release the guard ref that prevented close.
	_ = fc.Release(relPath)
	// Release the caller's actual ref.
	releaseErr := fc.Release(relPath)

	if syncErr != nil {
		return fmt.Errorf(
			"sync %s: %w", relPath, syncErr,
		)
	}
	return releaseErr
}

// Close releases all cached file handles.
func (fc *FileCache) Close() error {
	var firstErr error
	for i := range fc.shards {
		s := &fc.shards[i]
		s.mu.Lock()
		for k, entry := range s.files {
			if err := entry.file.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
			delete(s.files, k)
		}
		s.mu.Unlock()
	}
	return firstErr
}

// CephFSReader implements pipeline.DataReader for
// CephFS files mounted under baseDir.
// Uses a shared FileCache for multi-file concurrent
// access. CloseFile releases the handle after a
// CommitRequest via drainPending.
type CephFSReader struct {
	cache    *FileCache
	acquired sync.Map
}

// newCephFSReader creates a reader rooted at baseDir.
// Production code passes constant.DataMountPath.
func newCephFSReader(baseDir string) *CephFSReader {
	return &CephFSReader{
		cache: NewReadCache(baseDir),
	}
}

// NewCephFSReader creates a reader for production use
// (rooted at constant.DataMountPath).
func NewCephFSReader() *CephFSReader {
	return newCephFSReader(constant.DataMountPath)
}

// ReadAt opens (or reuses) the file at
// baseDir/filePath and reads length bytes at offset.
func (r *CephFSReader) ReadAt(
	filePath string, offset, length int64,
) ([]byte, error) {
	f, err := r.cache.Acquire(filePath, 0)
	if err != nil {
		return nil, err
	}

	if _, loaded := r.acquired.LoadOrStore(filePath, struct{}{}); loaded {
		_ = r.cache.Release(filePath)
	}

	data := make([]byte, length)
	n, err := f.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf(
			"pread %s at %d: %w",
			filePath, offset, err,
		)
	}
	return data[:n], nil
}

// CloseFile releases the open handle for filePath.
// Called by StageSendData after sending a commit.
func (r *CephFSReader) CloseFile(
	filePath string,
) error {
	r.acquired.Delete(filePath)
	return r.cache.Release(filePath)
}

// Close releases all cached file handles.
func (r *CephFSReader) Close() error {
	return r.cache.Close()
}

var _ pipeline.DataReader = (*CephFSReader)(nil)
