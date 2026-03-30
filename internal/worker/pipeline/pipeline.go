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

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// ChangeBlock matches ceph.ChangeBlock to avoid importing ceph package (no CGO needed).
type ChangeBlock struct {
	FilePath  string
	Offset    int64
	Len       int64
	ReqID     uint64
	TotalSize int64
}

// BlockIterator abstracts the RBD diff iterator.
type BlockIterator interface {
	Next() (*ChangeBlock, bool)
	Close() error
}

// StreamFactory opens a new gRPC Sync stream.
// Called once per DataSendWorker goroutine.
type StreamFactory func(ctx context.Context) (
	grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	], error,
)

// HashStreamFactory opens a new bidi hash stream.
// Called once per HashSendWorker goroutine.
// nil skips hash dedup entirely.
type HashStreamFactory func(
	ctx context.Context,
) (
	grpc.BidiStreamingClient[
		apiv1.HashRequest,
		apiv1.HashResponse,
	], error,
)

// Pipeline orchestrates the 5-stage concurrent transfer pipeline.
type Pipeline struct {
	cfg Config
}

// New creates a Pipeline with the given config.
func New(cfg Config) *Pipeline {
	return &Pipeline{cfg: cfg}
}

// Run executes the full pipeline: feeder -> read -> hash -> sendHash -> compress -> sendData.
// The caller creates win and may share it with external components (e.g. commitDrainer).
func (p *Pipeline) Run(
	ctx context.Context,
	iter BlockIterator,
	reader DataReader,
	newStream StreamFactory,
	newHashStream HashStreamFactory,
	win *WindowSemaphore,
) error {
	p.cfg.SetDefaults()
	if err := p.cfg.validate(); err != nil {
		return err
	}

	cfg := &p.cfg

	memRaw := NewMemSemaphore(cfg.MaxRawMemoryBytes)

	chunkCh := make(chan Chunk, cfg.ReadChanBuf)
	readCh := make(chan ReadChunk, cfg.ReadChanBuf)
	mismatchCh := make(chan HashedChunk, cfg.MismatchChanBuf)
	compressedCh := make(chan CompressedChunk, cfg.CompressChanBuf)

	g, gctx := errgroup.WithContext(ctx)

	// Stage 0: Feeder - assigns reqIDs and emits Chunks
	g.Go(func() error {
		defer close(chunkCh)
		return feeder(gctx, iter, chunkCh)
	})

	// Stage 1: Read - parallel pread from device
	g.Go(func() error {
		defer close(readCh)
		return StageRead(gctx, cfg, memRaw, win, reader, chunkCh, readCh)
	})

	if newHashStream == nil {
		// Skip hash stages: forward all to mismatchCh
		g.Go(func() error {
			defer close(mismatchCh)
			return forwardReadToMismatch(gctx, memRaw, win, readCh, mismatchCh)
		})
	} else {
		hashedCh := make(chan HashedChunk, cfg.HashChanBuf)
		// Stage 2: Hash - SHA-256 computation
		g.Go(func() error {
			defer close(hashedCh)
			return StageHash(gctx, cfg, memRaw, win, readCh, hashedCh)
		})
		// Stage 3: SendHash - hash comparison + dedup
		g.Go(func() error {
			defer close(mismatchCh)
			return StageSendHash(gctx, cfg, memRaw, win, newHashStream, hashedCh, mismatchCh)
		})
	}

	// Stage 4: Compress - LZ4 compression
	g.Go(func() error {
		defer close(compressedCh)
		return StageCompress(gctx, cfg, memRaw, win, mismatchCh, compressedCh)
	})

	// Stage 5: SendData - batched gRPC sends
	g.Go(func() error {
		return StageSendData(gctx, cfg, memRaw, win, newStream, compressedCh)
	})

	return g.Wait()
}

// forwardReadToMismatch is used when hashClient is nil.
// It forwards all ReadChunks directly to mismatchCh,
// treating every block as a mismatch.
func forwardReadToMismatch(
	ctx context.Context,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	readCh <-chan ReadChunk,
	mismatchCh chan<- HashedChunk,
) error {
	for {
		var rc ReadChunk
		var ok bool
		select {
		case rc, ok = <-readCh:
			if !ok {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		length := rc.Length
		if !rc.IsZero {
			length = int64(len(rc.Data))
		}
		select {
		case mismatchCh <- HashedChunk{
			ReqID:     rc.ReqID,
			FilePath:  rc.FilePath,
			Offset:    rc.Offset,
			Length:    length,
			Data:      rc.Data,
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

func feeder(ctx context.Context, iter BlockIterator, chunkCh chan<- Chunk) error {
	for {
		block, ok := iter.Next()
		if !ok {
			return nil
		}

		select {
		case chunkCh <- Chunk{
			ReqID:     block.ReqID,
			FilePath:  block.FilePath,
			Offset:    block.Offset,
			Length:    block.Len,
			TotalSize: block.TotalSize,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
