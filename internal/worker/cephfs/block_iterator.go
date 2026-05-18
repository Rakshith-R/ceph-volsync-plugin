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

// maxBlockLen is the maximum byte length of a single
// changedBlock emitted by the iterator. CephFS snap-diff
// can return coalesced extents of 67 MB or more; splitting
// them here keeps every chunk within the pipeline's
// maxChunkSize limit.
const maxBlockLen uint64 = 16 * 1024 * 1024

// splitLargeBlocks splits any block whose Len exceeds
// maxLen into consecutive blocks of maxLen (the last
// block gets the remainder). Blocks that already fit are
// appended unchanged. The returned slice reuses the
// backing array of blocks when possible.
func splitLargeBlocks(blocks []changedBlock, maxLen uint64) []changedBlock {
	// Fast path: nothing to split.
	needsSplit := false
	for i := range blocks {
		if blocks[i].Len > maxLen {
			needsSplit = true
			break
		}
	}
	if !needsSplit {
		return blocks
	}

	out := blocks[:0]
	for _, b := range blocks {
		if b.Len <= maxLen {
			out = append(out, b)
			continue
		}
		off := b.Offset
		remaining := b.Len
		for remaining > 0 {
			l := remaining
			if l > maxLen {
				l = maxLen
			}
			out = append(out, changedBlock{Offset: off, Len: l})
			off += l
			remaining -= l
		}
	}
	return out
}

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
	files              []string
	fileCh             <-chan changedFileEntry
	ctx                context.Context
	fileIdx            int
	curIter            fileDiffIterator
	curFile            string
	buf                []changedBlock
	bufIdx             int
	reqID              uint64
	fileStartReqID     uint64
	totalSize          int64
	boundaryCh         chan<- fileBoundary
	pendingBoundaries  []fileBoundary
	failed             bool
	smallFileThreshold int64
}

// CephFSIterOpt is a functional option for
// NewCephFSBlockIterator.
type CephFSIterOpt func(*CephFSBlockIterator)

// WithBoundaryChan sets a channel that receives a
// fileBoundary after the last block of each file.
func WithBoundaryChan(
	ch chan<- fileBoundary,
) CephFSIterOpt {
	return func(it *CephFSBlockIterator) {
		it.boundaryCh = ch
	}
}

// WithFileChan sets a channel of changedFileEntry values as
// input instead of a static slice. The iterator reads
// entries from the channel and processes them as they
// arrive. Size is taken from changedFileEntry.size directly,
// avoiding a second stat call.
func WithFileChan(
	ch <-chan changedFileEntry,
) CephFSIterOpt {
	return func(it *CephFSBlockIterator) {
		it.fileCh = ch
	}
}

// WithSmallFileThreshold sets a byte threshold below
// which files are emitted as synthetic full-file blocks
// without opening a BlockDiffIterator.
func WithSmallFileThreshold(bytes int64) CephFSIterOpt {
	return func(it *CephFSBlockIterator) {
		it.smallFileThreshold = bytes
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
				it.buf = splitLargeBlocks(blocks, maxBlockLen)
				it.bufIdx = 0
				continue
			}
		}

		// Emit boundary for the previous file, but only
		// if blocks were actually emitted (reqID advanced).
		if it.curFile != "" && it.boundaryCh != nil && it.reqID > it.fileStartReqID {
			it.emitBoundary()
		}

		it.drainPendingBoundaries()

		it.closeCurrentIter()

		// Advance to next file.
		nextFile, ok := it.nextFile()
		if !ok {
			it.curFile = ""
			return nil, false
		}
		it.curFile = nextFile
		it.fileStartReqID = it.reqID

		// In channel mode totalSize is set from changedFileEntry.size.
		// Skip empty files and use synthetic blocks for small files.
		// In slice mode totalSize is zero (unknown); skip these checks.
		if it.fileCh != nil {
			if it.totalSize == 0 {
				// Empty file: no blocks to emit.
				continue
			}
			// Small file: emit synthetic full-file block(s)
			// without opening a BlockDiffIterator.
			// Negative totalSize means unknown size; fall through to block-diff.
			if it.smallFileThreshold > 0 && it.totalSize > 0 && it.totalSize <= it.smallFileThreshold {
				it.buf = splitLargeBlocks(
					[]changedBlock{{Offset: 0, Len: uint64(it.totalSize)}}, //nolint:gosec
					maxBlockLen,
				)
				it.bufIdx = 0
				continue
			}
		}

		iter, err := it.newIter(it.curFile)
		if err != nil {
			// Skip files that cannot be opened.
			continue
		}
		it.curIter = iter
	}
}

// emitBoundary buffers a fileBoundary for the current
// file. Boundaries are sent to boundaryCh
// non-blocking; overflow is stored in
// pendingBoundaries and drained on subsequent Next()
// calls, preventing deadlock when the pipeline
// feeder and commit drainer share the window.
func (it *CephFSBlockIterator) emitBoundary() {
	fb := fileBoundary{
		path:      it.curFile,
		lastReqID: it.reqID - 1,
		totalSize: it.totalSize,
	}
	select {
	case it.boundaryCh <- fb:
	default:
		it.pendingBoundaries = append(it.pendingBoundaries, fb)
	}
}

// drainPendingBoundaries flushes buffered boundaries
// to boundaryCh non-blocking.
func (it *CephFSBlockIterator) drainPendingBoundaries() {
	for len(it.pendingBoundaries) > 0 {
		select {
		case it.boundaryCh <- it.pendingBoundaries[0]:
			it.pendingBoundaries = it.pendingBoundaries[1:]
		default:
			return
		}
	}
}

// flushAllBoundaries sends all pending boundaries to
// boundaryCh, blocking if necessary. Safe to call
// only after the pipeline has finished (all reqIDs
// acked), so the drainer will consume entries.
func (it *CephFSBlockIterator) flushAllBoundaries() {
	for _, fb := range it.pendingBoundaries {
		if it.ctx != nil {
			select {
			case it.boundaryCh <- fb:
			case <-it.ctx.Done():
				return
			}
		} else {
			it.boundaryCh <- fb
		}
	}
	it.pendingBoundaries = nil
}

// nextFile returns the next file path to process.
// In channel mode, blocks until a changedFileEntry arrives
// or the channel closes; size is stored in totalSize.
// In slice mode, indexes into the files slice.
func (it *CephFSBlockIterator) nextFile() (
	string, bool,
) {
	if it.fileCh != nil {
		var fe changedFileEntry
		var ok bool
		if it.ctx != nil {
			select {
			case fe, ok = <-it.fileCh:
			case <-it.ctx.Done():
				return "", false
			}
		} else {
			fe, ok = <-it.fileCh
		}
		if !ok {
			return "", false
		}
		it.totalSize = fe.size
		return fe.path, true
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

// Close releases any open iterator, emits the final
// file boundary if applicable, and flushes all
// pending boundaries to boundaryCh.
func (it *CephFSBlockIterator) Close() error {
	if !it.failed && it.curFile != "" &&
		it.boundaryCh != nil && it.reqID > it.fileStartReqID {
		it.emitBoundary()
	}
	if it.boundaryCh != nil {
		it.flushAllBoundaries()
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
