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

package cephfs

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

// commitDrainer monitors file boundary events from the
// iterator and sends batched CommitRequests when files
// are safe to commit (all blocks acked by destination).
type commitDrainer struct {
	boundaryCh   <-chan fileBoundary
	reader       pipeline.DataReader
	commitStream grpc.BidiStreamingClient[
		apiv1.CommitRequest, apiv1.CommitResponse,
	]
	win         *pipeline.WindowSemaphore
	maxWindow   int
	committedCh chan<- string
}

// Run processes file boundary events until boundaryCh
// is closed, then flushes all remaining pending files.
func (d *commitDrainer) Run(
	ctx context.Context,
) error {
	defer func() { _ = d.commitStream.CloseSend() }()

	var pending []fileBoundary
	var currentReqID uint64

	for {
		select {
		case fb, ok := <-d.boundaryCh:
			if !ok {
				return d.flushAll(ctx, pending)
			}
			currentReqID = fb.lastReqID
			pending = append(pending, fb)
			if err := d.drainQualified(
				ctx, &pending, currentReqID,
			); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// drainQualified sends commits for files that satisfy
// both the distance check and ack check.
func (d *commitDrainer) drainQualified(
	ctx context.Context,
	pending *[]fileBoundary,
	currentReqID uint64,
) error {
	var batch []*apiv1.CommitEntry
	var committed []string

	for len(*pending) > 0 {
		head := (*pending)[0]
		// Distance check (fast filter, no lock)
		if currentReqID <
			head.lastReqID+
				uint64(d.maxWindow)+2 { //nolint:gosec // G115: positive window
			break
		}
		// Ack check: confirm destination processed
		if !d.win.IsReleased(head.lastReqID) {
			break
		}
		batch = append(batch, &apiv1.CommitEntry{
			Path:      head.path,
			TotalSize: uint64(head.totalSize), //nolint:gosec // G115: non-negative size
		})
		_ = d.reader.CloseFile(head.path)
		committed = append(committed, head.path)
		*pending = (*pending)[1:]
	}

	if len(batch) == 0 {
		return nil
	}

	if err := d.sendCommitBatch(ctx, batch); err != nil {
		return err
	}

	return d.notifyCommitted(ctx, committed)
}

// flushAll waits for all pending files to be acked,
// then sends a final batched commit.
func (d *commitDrainer) flushAll(
	ctx context.Context,
	pending []fileBoundary,
) error {
	for i := range pending {
		for !d.win.IsReleased(pending[i].lastReqID) {
			runtime.Gosched()
			time.Sleep(100 * time.Microsecond)
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
	}

	var batch []*apiv1.CommitEntry
	var committed []string
	for _, fb := range pending {
		batch = append(batch, &apiv1.CommitEntry{
			Path:      fb.path,
			TotalSize: uint64(fb.totalSize), //nolint:gosec // G115: non-negative size
		})
		_ = d.reader.CloseFile(fb.path)
		committed = append(committed, fb.path)
	}

	if len(batch) == 0 {
		return nil
	}

	if err := d.sendCommitBatch(ctx, batch); err != nil {
		return err
	}

	return d.notifyCommitted(ctx, committed)
}

// notifyCommitted sends committed file paths to the
// committedCh for metadata rsync. Only sends if
// committedCh is set.
func (d *commitDrainer) notifyCommitted(
	ctx context.Context,
	paths []string,
) error {
	if d.committedCh == nil {
		return nil
	}
	for _, p := range paths {
		select {
		case d.committedCh <- p:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (d *commitDrainer) sendCommitBatch(
	_ context.Context,
	batch []*apiv1.CommitEntry,
) error {
	if err := d.commitStream.Send(
		&apiv1.CommitRequest{Entries: batch},
	); err != nil {
		return fmt.Errorf("send commit: %w", err)
	}
	if _, err := d.commitStream.Recv(); err != nil {
		return fmt.Errorf("recv commit ack: %w", err)
	}
	return nil
}
