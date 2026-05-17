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
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// StreamFactory opens a new gRPC Sync stream.
// Called once per DataSendWorker goroutine.
type StreamFactory func(ctx context.Context) (
	grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	], error,
)

// ChangeBlock matches ceph.ChangeBlock to avoid importing ceph package (no CGO needed).
type ChangeBlock struct {
	FilePath  string
	Offset    int64
	Len       int64
	ReqID     uint64
	TotalSize int64
	IsZero    bool
}

// BlockIterator abstracts the RBD diff iterator.
type BlockIterator interface {
	Next() (*ChangeBlock, bool)
	Close() error
}

// Pipeline orchestrates the concurrent transfer pipeline.
type Pipeline struct {
	cfg   Config
	Stats Stats
}

// New creates a Pipeline with the given config.
func New(cfg Config) *Pipeline {
	return &Pipeline{cfg: cfg}
}

// Run executes the full pipeline: feeder -> read -> sendData.
// The caller creates win and may share it with external components (e.g. commitDrainer).
func (p *Pipeline) Run(
	ctx context.Context,
	iter BlockIterator,
	reader DataReader,
	newStream StreamFactory,
	win *WindowSemaphore,
) error {
	p.cfg.SetDefaults()
	if err := p.cfg.validate(); err != nil {
		return err
	}

	p.Stats.PipelineStart = time.Now()
	defer func() { p.Stats.PipelineEnd = time.Now() }()

	cfg := &p.cfg
	stats := &p.Stats

	memRaw := NewMemSemaphore(cfg.MaxRawMemoryBytes)

	chunkCh := make(chan Chunk, cfg.ReadChanBuf)
	readCh := make(chan ReadChunk, cfg.ReadChanBuf)

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		defer close(chunkCh)
		return feeder(gctx, iter, chunkCh)
	})

	g.Go(func() error {
		defer close(readCh)
		return StageRead(gctx, cfg, stats, memRaw, win, reader, chunkCh, readCh)
	})

	g.Go(func() error {
		return StageSendData(gctx, cfg, stats, memRaw, win, newStream, readCh)
	})

	return g.Wait()
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
			IsZero:    block.IsZero,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
