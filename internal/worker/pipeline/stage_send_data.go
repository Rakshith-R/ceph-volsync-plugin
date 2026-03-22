package pipeline

import (
	"context"
	"fmt"
	"io"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func StageSendData(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	newStream StreamFactory,
	inCh <-chan CompressedChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)
	for range cfg.DataSendWorkers {
		g.Go(func() error {
			stream, err := newStream(gctx)
			if err != nil {
				return fmt.Errorf(
					"open sync stream: %w", err,
				)
			}
			return dataSendWorker(
				gctx, cfg, memRaw, win,
				stream, inCh,
			)
		})
	}
	return g.Wait()
}

func dataSendWorker(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	stream grpc.BidiStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
	inCh <-chan CompressedChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)

	// Ack receiver: reads SyncResponse, releases window
	g.Go(func() error {
		return ackReceiver(gctx, win, stream)
	})

	// Sender: batches and sends WriteRequests
	g.Go(func() error {
		return dataSender(
			gctx, cfg, memRaw, stream, inCh,
		)
	})

	return g.Wait()
}

func ackReceiver(
	_ context.Context,
	win *WindowSemaphore,
	stream grpc.BidiStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
) error {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf(
				"recv sync ack: %w", err,
			)
		}
		for _, id := range resp.AcknowledgedIds {
			win.Release(id)
		}
	}
}

func dataSender(
	ctx context.Context,
	cfg *Config,
	memRaw *MemSemaphore,
	stream grpc.BidiStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
	inCh <-chan CompressedChunk,
) error {
	defer func() { _ = stream.CloseSend() }()

	var (
		blocks  []*apiv1.ChangedBlock
		pending []held
		accum   int
		curPath string
	)

	flush := func() error {
		if len(blocks) == 0 {
			return nil
		}

		req := &apiv1.SyncRequest{
			Operation: &apiv1.SyncRequest_Write{
				Write: &apiv1.WriteRequest{
					Path:   curPath,
					Blocks: blocks,
				},
			},
		}

		if err := stream.Send(req); err != nil {
			for i := range pending {
				pending[i].release(memRaw, nil)
			}
			return err
		}

		for i := range pending {
			pending[i].releaseMemOnly(memRaw)
		}

		blocks = nil
		pending = nil
		accum = 0
		return nil
	}

	for {
		var cc CompressedChunk
		var ok bool
		select {
		case cc, ok = <-inCh:
			if !ok {
				return flush()
			}
		case <-ctx.Done():
			for i := range pending {
				pending[i].release(memRaw, nil)
			}
			return ctx.Err()
		}

		// File boundary: flush current batch
		if curPath != "" && cc.FilePath != curPath {
			if err := flush(); err != nil {
				return err
			}
		}

		curPath = cc.FilePath

		algo := apiv1.CompressionAlgo_COMPRESSION_LZ4
		if cc.IsRaw {
			algo = apiv1.CompressionAlgo_COMPRESSION_NONE
		}

		block := &apiv1.ChangedBlock{
			Offset:      uint64(cc.Offset),             //nolint:gosec // G115: non-negative offset
			Length:      uint64(cc.UncompressedLength), //nolint:gosec // G115: non-negative length
			Data:        cc.Data,
			RequestId:   cc.ReqID,
			Compression: algo,
			TotalSize:   uint64(cc.TotalSize), //nolint:gosec // G115: non-negative size
		}

		blocks = append(blocks, block)
		pending = append(pending, cc.Held)
		accum += len(cc.Data)

		if accum >= int(cfg.DataBatchMaxBytes) ||
			len(blocks) >= cfg.DataBatchMaxCount {
			if err := flush(); err != nil {
				return err
			}
		}
	}
}
