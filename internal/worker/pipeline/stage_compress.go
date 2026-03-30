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

	"github.com/pierrec/lz4/v4"
	"golang.org/x/sync/errgroup"
)

func StageCompress(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	inCh <-chan HashedChunk,
	outCh chan<- CompressedChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)

	for range cfg.CompressWorkers {
		g.Go(func() error {
			return compressWorker(
				gctx, memRaw, win, inCh, outCh,
			)
		})
	}

	return g.Wait()
}

func compressWorker(
	ctx context.Context,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
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
		if hc.IsZero {
			// Zero-filled chunk: no data to compress,
			// but preserve the actual block length.
			uncompLen = hc.Length
		}

		var dst []byte
		var n int
		isRaw := false

		if hc.IsZero {
			// Nothing to compress (zero chunk).
			dst = nil
			isRaw = true
		} else {
			maxDst := lz4.CompressBlockBound(len(hc.Data))
			dst = make([]byte, maxDst)
			var err error
			n, err = lz4.CompressBlock(hc.Data, dst, nil)

			if err != nil || n == 0 || n >= len(hc.Data) {
				dst = hc.Data
				n = len(hc.Data)
				isRaw = true
			} else {
				dst = dst[:n]
			}
		}

		saved := int64(len(hc.Data)) - int64(n)
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
			IsZero:             hc.IsZero,
			TotalSize:          hc.TotalSize,
			Held:               hc.Held,
		}:
		case <-ctx.Done():
			hc.Held.release(memRaw, win)
			return ctx.Err()
		}
	}
}
