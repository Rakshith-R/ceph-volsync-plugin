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

// CephFSBlockIterator implements pipeline.BlockIterator.
// It flattens block diffs across all large changed files.
type CephFSBlockIterator struct {
	newIter func(relPath string) (
		fileDiffIterator, error,
	)
	files   []string
	fileIdx int
	curIter fileDiffIterator
	curFile string
	buf     []changedBlock
	bufIdx  int
}

// NewCephFSBlockIterator creates an iterator over the
// given file paths. newIter is called once per file to
// open its block diff iterator. newIter may be nil only
// when files is also nil (empty case).
func NewCephFSBlockIterator(
	newIter func(string) (fileDiffIterator, error),
	files []string,
) *CephFSBlockIterator {
	return &CephFSBlockIterator{
		newIter: newIter,
		files:   files,
	}
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
			return &pipeline.ChangeBlock{
				FilePath: it.curFile,
				Offset:   int64(b.Offset), //nolint:gosec
				Len:      int64(b.Len),    //nolint:gosec
			}, true
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
		it.closeCurrentIter()
		it.curFile = it.files[it.fileIdx]
		it.fileIdx++

		iter, err := it.newIter(it.curFile)
		if err != nil {
			// Skip files that cannot be opened.
			continue
		}
		it.curIter = iter
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

// Close releases any open iterator.
func (it *CephFSBlockIterator) Close() error {
	it.closeCurrentIter()
	return nil
}

var _ pipeline.BlockIterator = (*CephFSBlockIterator)(nil)
