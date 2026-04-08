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
	"sync"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/sync/errgroup"
)

// zeroHashCache caches xxhash-64 hashes of zero-filled
// buffers keyed by length, avoiding per-chunk allocation.
var zeroHashCache sync.Map

func zeroHash(length int64) uint64 {
	if v, ok := zeroHashCache.Load(length); ok {
		return v.(uint64)
	}
	h := xxhash.Sum64(make([]byte, length))
	zeroHashCache.Store(length, h)
	return h
}

// StageHash spawns HashWorkers goroutines that compute xxhash-64 of each ReadChunk.
func StageHash(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	inCh <-chan ReadChunk,
	outCh chan<- HashedChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)

	for range cfg.HashWorkers {
		g.Go(func() error {
			return hashWorker(gctx, memRaw, win, inCh, outCh)
		})
	}

	return g.Wait()
}

func hashWorker(
	ctx context.Context,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
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

		var hash uint64
		if rc.IsZero {
			hash = zeroHash(rc.Length)
		} else {
			hash = xxhash.Sum64(rc.Data)
		}

		length := rc.Length
		if !rc.IsZero {
			length = int64(len(rc.Data))
		}

		select {
		case outCh <- HashedChunk{
			ReqID:     rc.ReqID,
			FilePath:  rc.FilePath,
			Offset:    rc.Offset,
			Length:    length,
			Data:      rc.Data,
			Hash:      hash,
			IsZero:    rc.IsZero,
			TotalSize: rc.TotalSize,
			Held:      rc.Held,
		}:
		case <-ctx.Done():
			rc.Held.release(memRaw, win)
			return ctx.Err()
		}
	}
}
