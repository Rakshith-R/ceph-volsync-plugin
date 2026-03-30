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
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// StageRead spawns ReadWorkers goroutines that read chunks from device,
// detect zeros, and emit to readCh. Zero blocks have IsZero=true and
// nil Data; their memRaw is released early to reduce memory pressure.
func StageRead(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	reader DataReader,
	inCh <-chan Chunk,
	readCh chan<- ReadChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)

	for range cfg.ReadWorkers {
		g.Go(func() error {
			return readWorker(gctx, cfg, memRaw, win, reader, inCh, readCh)
		})
	}

	return g.Wait()
}

func readWorker(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	reader DataReader,
	inCh <-chan Chunk,
	readCh chan<- ReadChunk,
) error {
	for {
		var chunk Chunk
		var ok bool
		select {
		case chunk, ok = <-inCh:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}

		if err := memRaw.Acquire(ctx, cfg.ChunkSize); err != nil {
			return err
		}

		if err := win.Acquire(ctx, chunk.ReqID); err != nil {
			memRaw.Release(cfg.ChunkSize)
			return err
		}

		data, err := reader.ReadAt(
			chunk.FilePath, chunk.Offset, chunk.Length,
		)
		if err != nil {
			memRaw.Release(cfg.ChunkSize)
			win.Release(chunk.ReqID)
			return fmt.Errorf("read chunk %d: %w", chunk.ReqID, err)
		}

		if common.IsAllZero(data) {
			memRaw.Release(cfg.ChunkSize)
			// Win stays acquired; released by ack receiver
			select {
			case readCh <- ReadChunk{
				ReqID:     chunk.ReqID,
				FilePath:  chunk.FilePath,
				Offset:    chunk.Offset,
				Length:    chunk.Length,
				Data:      nil,
				IsZero:    true,
				TotalSize: chunk.TotalSize,
				Held:      held{reqID: chunk.ReqID, hasWin: true},
			}:
			case <-ctx.Done():
				win.Release(chunk.ReqID)
				return ctx.Err()
			}
			continue
		}

		h := held{
			reqID:   chunk.ReqID,
			memRawN: cfg.ChunkSize,
			hasWin:  true,
			hasMem:  true,
		}

		select {
		case readCh <- ReadChunk{
			ReqID:     chunk.ReqID,
			FilePath:  chunk.FilePath,
			Offset:    chunk.Offset,
			Length:    int64(len(data)),
			Data:      data,
			TotalSize: chunk.TotalSize,
			Held:      h,
		}:
		case <-ctx.Done():
			h.release(memRaw, win)
			return ctx.Err()
		}
	}
}
