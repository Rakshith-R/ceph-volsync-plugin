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

	"golang.org/x/sync/errgroup"
)

// StageHash spawns HashWorkers goroutines that compute SHA-256 of each ReadChunk.
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

		var hash [32]byte
		if rc.IsZero {
			zeroBuf := make([]byte, rc.Length)
			hash = sha256.Sum256(zeroBuf)
		} else {
			hash = sha256.Sum256(rc.Data)
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
