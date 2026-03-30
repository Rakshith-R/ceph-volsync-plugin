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
		apiv1.WriteRequest, apiv1.WriteResponse,
	],
	inCh <-chan CompressedChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)

	// Ack receiver: reads WriteResponse, releases window
	g.Go(func() error {
		return ackReceiver(gctx, win, stream)
	})

	// Sender: batches and sends WriteRequests
	g.Go(func() error {
		return dataSender(
			gctx, cfg, memRaw, win, stream, inCh,
		)
	})

	return g.Wait()
}

func ackReceiver(
	_ context.Context,
	win *WindowSemaphore,
	stream grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
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
	win *WindowSemaphore,
	stream grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	],
	inCh <-chan CompressedChunk,
) error {
	defer func() { _ = stream.CloseSend() }()

	var (
		blocks      []*apiv1.ChangedBlock
		pending     []held
		accum       int
		currentPath string
	)

	flush := func() error {
		if len(blocks) == 0 {
			return nil
		}

		req := &apiv1.WriteRequest{
			Blocks: blocks,
		}

		if err := stream.Send(req); err != nil {
			for i := range pending {
				pending[i].release(memRaw, win)
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
				pending[i].release(memRaw, win)
			}
			return ctx.Err()
		}

		// Flush on path change
		if cc.FilePath != currentPath && len(blocks) > 0 {
			if err := flush(); err != nil {
				cc.Held.release(memRaw, win)
				return err
			}
		}
		currentPath = cc.FilePath

		algo := apiv1.CompressionAlgo_COMPRESSION_LZ4
		if cc.IsRaw {
			algo = apiv1.CompressionAlgo_COMPRESSION_NONE
		}

		block := &apiv1.ChangedBlock{
			RequestId:   cc.ReqID,
			FilePath:    cc.FilePath,
			TotalSize:   uint64(cc.TotalSize),          //nolint:gosec // G115: non-negative size
			Offset:      uint64(cc.Offset),             //nolint:gosec // G115: non-negative offset
			Length:      uint64(cc.UncompressedLength), //nolint:gosec // G115: non-negative length
			IsZero:      cc.IsZero,
			Data:        cc.Data,
			Compression: algo,
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
