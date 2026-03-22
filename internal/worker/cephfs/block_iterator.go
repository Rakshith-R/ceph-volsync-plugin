package cephfs

import (
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
// It flattens block diffs across all large changed files.
type CephFSBlockIterator struct {
	newIter func(relPath string) (
		fileDiffIterator, error,
	)
	files      []string
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

		// Advance to next file.
		if it.fileIdx >= len(it.files) {
			return nil, false
		}

		// Emit boundary for the previous file.
		if it.curFile != "" && it.boundaryCh != nil {
			select {
			case it.boundaryCh <- fileBoundary{
				path:      it.curFile,
				lastReqID: it.reqID - 1,
				totalSize: it.totalSize,
			}:
			default:
			}
		}

		it.closeCurrentIter()
		it.curFile = it.files[it.fileIdx]
		it.fileIdx++

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
		select {
		case it.boundaryCh <- fileBoundary{
			path:      it.curFile,
			lastReqID: it.reqID - 1,
			totalSize: it.totalSize,
		}:
		default:
		}
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
