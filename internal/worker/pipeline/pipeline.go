package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// ChangeBlock matches ceph.ChangeBlock to avoid importing ceph package (no CGO needed).
type ChangeBlock struct {
	FilePath string
	Offset   int64
	Len      int64
}

// BlockIterator abstracts the RBD diff iterator.
type BlockIterator interface {
	Next() (*ChangeBlock, bool)
	Close() error
}

// Pipeline orchestrates the 5-stage concurrent transfer pipeline.
type Pipeline struct {
	cfg Config
}

// New creates a Pipeline with the given config.
func New(cfg Config) *Pipeline {
	return &Pipeline{cfg: cfg}
}

// Run executes the full pipeline: feeder -> read -> hash -> sendHash -> compress -> sendData.
func (p *Pipeline) Run(
	ctx context.Context,
	iter BlockIterator,
	reader DataReader,
	stream grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse],
	hashClient apiv1.HashServiceClient,
) error {
	p.cfg.setDefaults()
	if err := p.cfg.validate(); err != nil {
		return err
	}

	cfg := &p.cfg

	memRaw := NewMemSemaphore(cfg.MaxRawMemoryBytes)
	win := NewWindowSemaphore(cfg.MaxWindow)

	chunkCh := make(chan Chunk, cfg.ReadChanBuf)
	readCh := make(chan ReadChunk, cfg.ReadChanBuf)
	zeroCh := make(chan ZeroChunk, cfg.ZeroChanBuf)
	hashedCh := make(chan HashedChunk, cfg.HashChanBuf)
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
		defer close(zeroCh)
		return StageRead(gctx, cfg, memRaw, win, reader, chunkCh, readCh, zeroCh)
	})

	// Stage 2: Hash - SHA-256 computation
	g.Go(func() error {
		defer close(hashedCh)
		return StageHash(gctx, cfg, readCh, hashedCh)
	})

	// Stage 3: SendHash - hash comparison + dedup
	g.Go(func() error {
		defer close(mismatchCh)
		return StageSendHash(gctx, cfg, memRaw, win, hashClient, hashedCh, zeroCh, mismatchCh)
	})

	// Stage 4: Compress - LZ4 compression
	g.Go(func() error {
		defer close(compressedCh)
		return StageCompress(gctx, cfg, memRaw, mismatchCh, compressedCh)
	})

	// Stage 5: SendData - batched gRPC sends
	g.Go(func() error {
		return StageSendData(gctx, cfg, memRaw, win, stream, compressedCh, reader)
	})

	return g.Wait()
}

func feeder(ctx context.Context, iter BlockIterator, chunkCh chan<- Chunk) error {
	var reqID uint64
	for {
		block, ok := iter.Next()
		if !ok {
			return nil
		}

		select {
		case chunkCh <- Chunk{
			ReqID:    reqID,
			FilePath: block.FilePath,
			Offset:   block.Offset,
			Length:   block.Len,
		}:
			reqID++
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
