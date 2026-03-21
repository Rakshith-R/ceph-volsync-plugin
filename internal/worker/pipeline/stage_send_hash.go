package pipeline

import (
	"context"
	"crypto/sha256"

	"golang.org/x/sync/errgroup"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func StageSendHash(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	hashClient apiv1.HashServiceClient,
	hashedCh <-chan HashedChunk,
	zeroCh <-chan ZeroChunk,
	mismatchCh chan<- HashedChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)
	batchCh := make(chan []HashedChunk, cfg.HashSendWorkers*2)

	g.Go(func() error {
		defer close(batchCh)
		return hashBatcher(gctx, cfg, win, hashedCh, zeroCh, batchCh)
	})

	for range cfg.HashSendWorkers {
		g.Go(func() error {
			return hashSender(gctx, memRaw, win, hashClient, batchCh, mismatchCh)
		})
	}

	return g.Wait()
}

func hashBatcher(
	ctx context.Context,
	cfg *Config,
	win *WindowSemaphore,
	hashedCh <-chan HashedChunk,
	zeroCh <-chan ZeroChunk,
	batchCh chan<- []HashedChunk,
) error {
	var batch []HashedChunk
	pressure := win.PressureSignal(cfg.WinPressureThresh)

	zeroBuf := make([]byte, cfg.ChunkSize)
	zeroHash := sha256.Sum256(zeroBuf)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		b := batch
		batch = nil
		select {
		case batchCh <- b:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	hashedOpen := true
	zeroOpen := true

	for hashedOpen || zeroOpen {
		select {
		case hc, ok := <-hashedCh:
			if !ok {
				hashedCh = nil
				hashedOpen = false
				continue
			}
			batch = append(batch, hc)
			if len(batch) >= cfg.HashBatchMaxCount {
				if err := flush(); err != nil {
					return err
				}
			}
		case zc, ok := <-zeroCh:
			if !ok {
				zeroCh = nil
				zeroOpen = false
				continue
			}
			batch = append(batch, HashedChunk{
				ReqID:    zc.ReqID,
				FilePath: zc.FilePath,
				Offset:   zc.Offset,
				Length:   zc.Length,
				Data:     nil,
				Hash:     zeroHash,
			})
			if len(batch) >= cfg.HashBatchMaxCount {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-pressure:
			if err := flush(); err != nil {
				return err
			}
			pressure = win.PressureSignal(cfg.WinPressureThresh)
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return flush()
}

func hashSender(
	ctx context.Context,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	hashClient apiv1.HashServiceClient,
	batchCh <-chan []HashedChunk,
	mismatchCh chan<- HashedChunk,
) error {
	for {
		var batch []HashedChunk
		var ok bool
		select {
		case batch, ok = <-batchCh:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		req := &apiv1.HashBatchRequest{
			Hashes: make([]*apiv1.BlockHash, len(batch)),
		}
		for i, hc := range batch {
			req.Hashes[i] = &apiv1.BlockHash{
				RequestId: hc.ReqID,
				FilePath:  hc.FilePath,
				Offset:    uint64(hc.Offset),
				Length:    uint64(hc.Length),
				Sha256:    hc.Hash[:],
			}
		}

		resp, err := hashClient.CompareHashes(ctx, req)
		if err != nil {
			return err
		}

		mismatched := make(map[uint64]struct{}, len(resp.MismatchedIds))
		for _, id := range resp.MismatchedIds {
			mismatched[id] = struct{}{}
		}

		for _, hc := range batch {
			if _, isMiss := mismatched[hc.ReqID]; isMiss {
				select {
				case mismatchCh <- hc:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				hc.Held.release(memRaw, nil, win)
			}
		}
	}
}
