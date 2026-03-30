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

package pipeline

import (
	"context"
	"crypto/sha256"
	"fmt"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func StageSendHash(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	newHashStream HashStreamFactory,
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
			hs, err := newHashStream(gctx)
			if err != nil {
				return fmt.Errorf("open hash stream: %w", err)
			}
			return hashSender(gctx, memRaw, win, hs, batchCh, mismatchCh)
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

	// zeroHash is computed per-chunk below, since trailing
	// blocks may have Length < ChunkSize.

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
			zeroBuf := make([]byte, zc.Length)
			zHash := sha256.Sum256(zeroBuf)
			batch = append(batch, HashedChunk{
				ReqID:     zc.ReqID,
				FilePath:  zc.FilePath,
				Offset:    zc.Offset,
				Length:    zc.Length,
				Data:      nil,
				Hash:      zHash,
				TotalSize: zc.TotalSize,
				Held:      zc.Held,
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
	stream grpc.BidiStreamingClient[
		apiv1.HashRequest,
		apiv1.HashResponse,
	],
	batchCh <-chan []HashedChunk,
	mismatchCh chan<- HashedChunk,
) error {
	defer func() { _ = stream.CloseSend() }()

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

		req := &apiv1.HashRequest{
			Hashes: make([]*apiv1.BlockHash, len(batch)),
		}
		for i, hc := range batch {
			req.Hashes[i] = &apiv1.BlockHash{
				RequestId: hc.ReqID,
				FilePath:  hc.FilePath,
				Offset:    uint64(hc.Offset), //nolint:gosec // G115: non-negative offset
				Length:    uint64(hc.Length), //nolint:gosec // G115: non-negative length
				Sha256:    hc.Hash[:],
				TotalSize: uint64(hc.TotalSize), //nolint:gosec // G115: non-negative size
			}
		}

		if err := stream.Send(req); err != nil {
			return err
		}

		resp, err := stream.Recv()
		if err != nil {
			return err
		}

		mismatched := make(map[uint64]struct{}, len(resp.MismatchedIds))
		for _, id := range resp.MismatchedIds {
			mismatched[id] = struct{}{}
		}

		for i, hc := range batch {
			if _, isMiss := mismatched[hc.ReqID]; isMiss {
				select {
				case mismatchCh <- hc:
				case <-ctx.Done():
					hc.Held.release(memRaw, win)
					for _, rem := range batch[i+1:] {
						rem.Held.release(memRaw, win)
					}
					return ctx.Err()
				}
			} else {
				hc.Held.release(memRaw, win)
			}
		}
	}
}
