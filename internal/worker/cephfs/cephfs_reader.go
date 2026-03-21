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

// CephFSReader implements pipeline.DataReader for
// CephFS files mounted under baseDir.
// Caches the most recently used file handle.
// CloseFile releases the handle after a CommitRequest.
type CephFSReader struct {
	baseDir  string
	mu       sync.Mutex
	openPath string
	openFile *os.File
}

// newCephFSReader creates a reader rooted at baseDir.
// Production code passes common.DataMountPath.
func newCephFSReader(baseDir string) *CephFSReader {
	return &CephFSReader{baseDir: baseDir}
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
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.openPath != filePath {
		if r.openFile != nil {
			_ = r.openFile.Close()
			r.openFile = nil
		}
		full := filepath.Join(r.baseDir, filePath)
		f, err := os.Open(full) //nolint:gosec // G304
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", full, err)
		}
		r.openPath = filePath
		r.openFile = f
	}

	data := make([]byte, length)
	n, err := r.openFile.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("pread %s at %d: %w", filePath, offset, err)
	}
	return data[:n], nil
}

// CloseFile releases the open handle for filePath.
// Called by StageSendData after sending a commit.
func (r *CephFSReader) CloseFile(filePath string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.openPath == filePath && r.openFile != nil {
		err := r.openFile.Close()
		r.openFile = nil
		r.openPath = ""
		return err
	}
	return nil
}

// Close releases the cached file handle.
func (r *CephFSReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.openFile != nil {
		err := r.openFile.Close()
		r.openFile = nil
		r.openPath = ""
		return err
	}
	return nil
}

var _ pipeline.DataReader = (*CephFSReader)(nil)
