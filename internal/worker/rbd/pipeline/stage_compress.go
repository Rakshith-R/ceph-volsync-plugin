package pipeline

import (
	"context"

	"github.com/pierrec/lz4/v4"
	"golang.org/x/sync/errgroup"
)

func StageCompress(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	inCh <-chan HashedChunk,
	outCh chan<- CompressedChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)

	for range cfg.CompressWorkers {
		g.Go(func() error {
			return compressWorker(gctx, memRaw, inCh, outCh)
		})
	}

	return g.Wait()
}

func compressWorker(
	ctx context.Context,
	memRaw *MemSemaphore,
	inCh <-chan HashedChunk,
	outCh chan<- CompressedChunk,
) error {
	for {
		var hc HashedChunk
		var ok bool
		select {
		case hc, ok = <-inCh:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		uncompLen := int64(len(hc.Data))

		maxDst := lz4.CompressBlockBound(len(hc.Data))
		dst := make([]byte, maxDst)
		n, err := lz4.CompressBlock(hc.Data, dst, nil)

		isRaw := false
		if err != nil || n == 0 || n >= len(hc.Data) {
			dst = hc.Data
			n = len(hc.Data)
			isRaw = true
		} else {
			dst = dst[:n]
		}

		saved := uncompLen - int64(n)
		if saved > 0 {
			hc.Held.partialReleaseMemRaw(memRaw, saved)
		}

		select {
		case outCh <- CompressedChunk{
			ReqID:              hc.ReqID,
			FilePath:           hc.FilePath,
			Offset:             hc.Offset,
			Data:               dst,
			Hash:               hc.Hash,
			UncompressedLength: uncompLen,
			IsRaw:              isRaw,
			Held:               hc.Held,
		}:
		case <-ctx.Done():
			hc.Held.release(memRaw, nil, nil)
			return ctx.Err()
		}
	}
}
