package pipeline

import (
	"context"
	"crypto/sha256"

	"golang.org/x/sync/errgroup"
)

// StageHash spawns HashWorkers goroutines that compute SHA-256 of each ReadChunk.
func StageHash(
	ctx context.Context,
	cfg *Config,
	inCh <-chan ReadChunk,
	outCh chan<- HashedChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)

	for range cfg.HashWorkers {
		g.Go(func() error {
			return hashWorker(gctx, inCh, outCh)
		})
	}

	return g.Wait()
}

func hashWorker(
	ctx context.Context,
	inCh <-chan ReadChunk,
	outCh chan<- HashedChunk,
) error {
	for {
		var rc ReadChunk
		var ok bool
		select {
		case rc, ok = <-inCh:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		hash := sha256.Sum256(rc.Data)

		select {
		case outCh <- HashedChunk{
			ReqID:     rc.ReqID,
			FilePath:  rc.FilePath,
			Offset:    rc.Offset,
			Length:    int64(len(rc.Data)),
			Data:      rc.Data,
			Hash:      hash,
			TotalSize: rc.TotalSize,
			Held:      rc.Held,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
