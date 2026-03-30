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

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

// fileDiffIterator abstracts go-ceph's per-file
// block diff iterator without importing CGO types.
// cephfs/source.go (with ceph_preview) wraps the
// concrete *ceph.BlockDiffIterator in this interface.
type fileDiffIterator interface {
	More() bool
	Read() ([]changedBlock, error)
	Close() error
}

// changedBlock mirrors ceph.ChangeBlock without CGO.
type changedBlock struct {
	Offset uint64
	Len    uint64
}

// fileBoundary records the last request ID and total
// size for a file after all its blocks have been
// emitted by the iterator.
type fileBoundary struct {
	path      string
	lastReqID uint64
	totalSize int64
}

// CephFSBlockIterator implements pipeline.BlockIterator.
// It flattens block diffs across all large changed
// files. Supports both slice-fed (files []string) and
// channel-fed (fileCh) input modes.
type CephFSBlockIterator struct {
	newIter func(relPath string) (
		fileDiffIterator, error,
	)
	files      []string
	fileCh     <-chan string
	ctx        context.Context
	fileIdx    int
	curIter    fileDiffIterator
	curFile    string
	buf        []changedBlock
	bufIdx     int
	reqID      uint64
	totalSize  int64
	sizeFn     func(string) (int64, error)
	boundaryCh chan<- fileBoundary
	failed     bool
}

// CephFSIterOpt is a functional option for
// NewCephFSBlockIterator.
type CephFSIterOpt func(*CephFSBlockIterator)

// WithSizeFunc sets a function that returns the total
// size of a file given its path.
func WithSizeFunc(
	fn func(string) (int64, error),
) CephFSIterOpt {
	return func(it *CephFSBlockIterator) {
		it.sizeFn = fn
	}
}

// WithBoundaryChan sets a channel that receives a
// fileBoundary after the last block of each file.
func WithBoundaryChan(
	ch chan<- fileBoundary,
) CephFSIterOpt {
	return func(it *CephFSBlockIterator) {
		it.boundaryCh = ch
	}
}

// WithFileChan sets a channel of file paths as input
// instead of a static slice. The iterator reads files
// from the channel and processes them as they arrive.
func WithFileChan(
	ch <-chan string,
) CephFSIterOpt {
	return func(it *CephFSBlockIterator) {
		it.fileCh = ch
	}
}

// WithContext sets a context for cancellation-aware
// blocking on channels. Required when using
// WithFileChan to allow context cancellation to
// unblock the iterator.
func WithContext(
	ctx context.Context,
) CephFSIterOpt {
	return func(it *CephFSBlockIterator) {
		it.ctx = ctx
	}
}

// NewCephFSBlockIterator creates an iterator over the
// given file paths. newIter is called once per file to
// open its block diff iterator. newIter may be nil only
// when files is also nil (empty case).
func NewCephFSBlockIterator(
	newIter func(string) (fileDiffIterator, error),
	files []string,
	opts ...CephFSIterOpt,
) *CephFSBlockIterator {
	it := &CephFSBlockIterator{
		newIter: newIter,
		files:   files,
	}
	for _, o := range opts {
		o(it)
	}
	return it
}

// Next returns the next changed block, or (nil, false)
// when exhausted.
func (it *CephFSBlockIterator) Next() (
	*pipeline.ChangeBlock, bool,
) {
	for {
		// Drain buffer for current file.
		if it.bufIdx < len(it.buf) {
			b := it.buf[it.bufIdx]
			it.bufIdx++
			cb := &pipeline.ChangeBlock{
				FilePath:  it.curFile,
				Offset:    int64(b.Offset), //nolint:gosec
				Len:       int64(b.Len),    //nolint:gosec
				ReqID:     it.reqID,
				TotalSize: it.totalSize,
			}
			it.reqID++
			return cb, true
		}

		// Try to read more from current iterator.
		if it.curIter != nil && it.curIter.More() {
			blocks, err := it.curIter.Read()
			if err != nil || len(blocks) == 0 {
				it.closeCurrentIter()
			} else {
				it.buf = blocks
				it.bufIdx = 0
				continue
			}
		}

		// Emit boundary for the previous file.
		if it.curFile != "" && it.boundaryCh != nil {
			if !it.emitBoundary() {
				it.curFile = ""
				return nil, false
			}
		}

		it.closeCurrentIter()

		// Advance to next file.
		nextFile, ok := it.nextFile()
		if !ok {
			it.curFile = ""
			return nil, false
		}
		it.curFile = nextFile

		iter, err := it.newIter(it.curFile)
		if err != nil {
			// Skip files that cannot be opened.
			continue
		}
		it.curIter = iter

		// Resolve total size for the new file.
		if it.sizeFn != nil {
			sz, sErr := it.sizeFn(it.curFile)
			if sErr == nil {
				it.totalSize = sz
			} else {
				it.totalSize = 0
			}
		} else {
			it.totalSize = 0
		}
	}
}

// emitBoundary sends a fileBoundary for the current
// file. Returns true on success, false if context was
// cancelled (only possible when ctx is set).
func (it *CephFSBlockIterator) emitBoundary() bool {
	fb := fileBoundary{
		path:      it.curFile,
		lastReqID: it.reqID - 1,
		totalSize: it.totalSize,
	}
	if it.ctx != nil {
		select {
		case it.boundaryCh <- fb:
			return true
		case <-it.ctx.Done():
			return false
		}
	}
	// Legacy non-blocking for slice mode.
	select {
	case it.boundaryCh <- fb:
	default:
	}
	return true
}

// nextFile returns the next file path to process.
// In channel mode, blocks until a file arrives or
// the channel closes. In slice mode, indexes into
// the files slice.
func (it *CephFSBlockIterator) nextFile() (
	string, bool,
) {
	if it.fileCh != nil {
		if it.ctx != nil {
			select {
			case f, ok := <-it.fileCh:
				return f, ok
			case <-it.ctx.Done():
				return "", false
			}
		}
		f, ok := <-it.fileCh
		return f, ok
	}
	if it.fileIdx >= len(it.files) {
		return "", false
	}
	f := it.files[it.fileIdx]
	it.fileIdx++
	return f, true
}

func (it *CephFSBlockIterator) closeCurrentIter() {
	if it.curIter != nil {
		_ = it.curIter.Close()
		it.curIter = nil
		it.buf = nil
		it.bufIdx = 0
	}
}

// Close releases any open iterator and emits the
// final file boundary if applicable.
func (it *CephFSBlockIterator) Close() error {
	if !it.failed && it.curFile != "" &&
		it.boundaryCh != nil {
		_ = it.emitBoundary()
	}
	it.closeCurrentIter()
	return nil
}

// SetFailed marks the iterator so that Close() will
// not emit a final file boundary. Call this when the
// pipeline errors before calling Close().
func (it *CephFSBlockIterator) SetFailed() {
	it.failed = true
}

var _ pipeline.BlockIterator = (*CephFSBlockIterator)(nil)
