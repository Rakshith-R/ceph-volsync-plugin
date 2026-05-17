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
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func StageSendData(
	ctx context.Context,
	cfg *Config,
	stats *Stats,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	newStream StreamFactory,
	inCh <-chan ReadChunk,
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
				gctx, cfg, stats, memRaw, win,
				stream, inCh,
			)
		})
	}
	return g.Wait()
}

func dataSendWorker(
	ctx context.Context,
	cfg *Config,
	stats *Stats,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	stream grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	],
	inCh <-chan ReadChunk,
) error {
	g, gctx := errgroup.WithContext(ctx)

	// Ack receiver: reads WriteResponse, releases window
	g.Go(func() error {
		return ackReceiver(gctx, stats, win, stream)
	})

	// Sender: batches and sends WriteRequests
	g.Go(func() error {
		return dataSender(
			gctx, cfg, stats, memRaw, win, stream, inCh,
		)
	})

	return g.Wait()
}

type recvResult struct {
	resp    *apiv1.WriteResponse
	err     error
	elapsed time.Duration
}

func ackReceiver(
	ctx context.Context,
	stats *Stats,
	win *WindowSemaphore,
	stream grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	],
) error {
	for {
		ch := make(chan recvResult, 1)
		go func() {
			t0 := time.Now()
			resp, err := stream.Recv()
			ch <- recvResult{resp, err, time.Since(t0)}
		}()

		var res recvResult
		select {
		case res = <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}

		stats.AckTimeNs.Add(res.elapsed.Nanoseconds())

		if res.err == io.EOF {
			return nil
		}
		if res.err != nil {
			return fmt.Errorf("recv sync ack: %w", res.err)
		}
		stats.AckCount.Add(int64(len(res.resp.AcknowledgedIds)))
		for _, id := range res.resp.AcknowledgedIds {
			win.Release(id)
		}
	}
}

func dataSender(
	ctx context.Context,
	cfg *Config,
	stats *Stats,
	memRaw *MemSemaphore,
	win *WindowSemaphore,
	stream grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	],
	inCh <-chan ReadChunk,
) error {
	defer func() { _ = stream.CloseSend() }()

	var (
		blocks  []*apiv1.ChangedBlock
		pending []held
		accum   int
	)

	flush := func() error {
		if len(blocks) == 0 {
			return nil
		}

		req := &apiv1.WriteRequest{
			Blocks: blocks,
		}

		t0 := time.Now()
		if err := stream.Send(req); err != nil {
			for i := range pending {
				pending[i].release(memRaw, win)
			}
			return err
		}
		stats.SendTimeNs.Add(time.Since(t0).Nanoseconds())
		stats.SendBytes.Add(int64(accum))
		stats.SendCount.Add(int64(len(blocks)))
		stats.SendFlushes.Add(1)

		for i := range pending {
			pending[i].releaseMemOnly(memRaw)
		}

		blocks = nil
		pending = pending[:0]
		accum = 0
		return nil
	}

	for {
		var cc ReadChunk
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

		block := &apiv1.ChangedBlock{
			RequestId: cc.ReqID,
			FilePath:  cc.FilePath,
			TotalSize: uint64(cc.TotalSize), //nolint:gosec // G115: non-negative size
			Offset:    uint64(cc.Offset),    //nolint:gosec // G115: non-negative offset
			Length:    uint64(cc.Length),    //nolint:gosec // G115: non-negative length
			IsZero:    cc.IsZero,
			Data:      cc.Data,
		}

		// Flush before appending if this block would
		// exceed the batch size limit.
		if len(blocks) > 0 &&
			(accum+len(cc.Data) >= int(cfg.DataBatchMaxBytes) ||
				len(blocks) >= cfg.DataBatchMaxCount) {
			if err := flush(); err != nil {
				cc.Held.release(memRaw, win)
				return err
			}
		}

		blocks = append(blocks, block)
		pending = append(pending, cc.Held)
		accum += len(cc.Data)
	}
}
