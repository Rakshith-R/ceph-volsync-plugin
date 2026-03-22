package cephfs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

type fileEntry struct {
	file     *os.File
	refCount int
}

// FileCache is a ref-counted file handle cache.
// Thread-safe for concurrent use across goroutines.
// A single instance is shared by all components
// that access files under the same baseDir.
type FileCache struct {
	baseDir string
	mode    int
	perm    os.FileMode
	mu      sync.Mutex
	files   map[string]*fileEntry
}

// NewFileCache creates a FileCache rooted at
// baseDir. mode and perm control os.OpenFile flags.
func NewFileCache(
	baseDir string, mode int, perm os.FileMode,
) *FileCache {
	return &FileCache{
		baseDir: baseDir,
		mode:    mode,
		perm:    perm,
		files:   make(map[string]*fileEntry),
	}
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
	fc.mu.Lock()
	defer fc.mu.Unlock()

	if entry, ok := fc.files[relPath]; ok {
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

	fc.files[relPath] = &fileEntry{
		file: f, refCount: 1,
	}
	return f, nil
}

// Release decrements refCount for relPath.
// Closes the file when refCount reaches 0.
func (fc *FileCache) Release(
	relPath string,
) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	entry, ok := fc.files[relPath]
	if !ok {
		return nil
	}
	entry.refCount--
	if entry.refCount <= 0 {
		delete(fc.files, relPath)
		return entry.file.Close()
	}
	return nil
}

// SyncAndRelease syncs the file then releases
// one reference. For destination write commits.
func (fc *FileCache) SyncAndRelease(
	relPath string,
) error {
	fc.mu.Lock()
	entry, ok := fc.files[relPath]
	fc.mu.Unlock()

	if !ok {
		return nil
	}
	if err := entry.file.Sync(); err != nil {
		return fmt.Errorf(
			"sync %s: %w", relPath, err,
		)
	}
	return fc.Release(relPath)
}

// Close releases all cached file handles.
func (fc *FileCache) Close() error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	var firstErr error
	for k, entry := range fc.files {
		if err := entry.file.Close(); err != nil &&
			firstErr == nil {
			firstErr = err
		}
		delete(fc.files, k)
	}
	return firstErr
}

// CephFSReader implements pipeline.DataReader for
// CephFS files mounted under baseDir.
// Uses a shared FileCache for multi-file concurrent
// access. CloseFile releases the handle after a
// CommitRequest via drainPending.
type CephFSReader struct {
	cache *FileCache
}

// newCephFSReader creates a reader rooted at baseDir.
// Production code passes common.DataMountPath.
func newCephFSReader(baseDir string) *CephFSReader {
	return &CephFSReader{
		cache: NewReadCache(baseDir),
	}
}

// NewCephFSReader creates a reader for production use
// (rooted at common.DataMountPath).
func NewCephFSReader() *CephFSReader {
	return newCephFSReader(common.DataMountPath)
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
	// Do NOT release here — file stays open
	// until CloseFile via drainPending.

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
	return r.cache.Release(filePath)
}

// Close releases all cached file handles.
func (r *CephFSReader) Close() error {
	return r.cache.Close()
}

var _ pipeline.DataReader = (*CephFSReader)(nil)
