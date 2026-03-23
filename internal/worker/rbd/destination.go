/*
Copyright 2025.

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

package rbd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/go-logr/logr"
	"github.com/pierrec/lz4/v4"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

// DestinationWorker represents an RBD destination
// worker instance.
type DestinationWorker struct {
	common.BaseDestinationWorker
}

// NewDestinationWorker creates a new RBD destination
// worker.
func NewDestinationWorker(
	logger logr.Logger, config common.DestinationConfig,
) *DestinationWorker {
	return &DestinationWorker{
		BaseDestinationWorker: common.BaseDestinationWorker{
			Logger: logger.WithName(
				"rbd-destination-worker",
			),
			Config: config,
		},
	}
}

// Run starts the RBD destination worker.
func (w *DestinationWorker) Run(
	ctx context.Context,
) error {
	dataServer := &RBDDataServer{
		logger:     w.Logger,
		devicePath: constant.DevicePath,
	}
	hashServer := &HashServer{
		logger:     w.Logger,
		devicePath: constant.DevicePath,
	}
	commitServer := &RBDCommitServer{
		logger:     w.Logger,
		devicePath: constant.DevicePath,
	}
	return w.RunWithHashAndCommit(
		ctx, dataServer, hashServer, commitServer,
	)
}

// RBDDataServer implements DataService for block
// devices.
type RBDDataServer struct {
	apiv1.UnimplementedDataServiceServer
	logger     logr.Logger
	devicePath string
}

// Sync handles a bidi-streaming RPC for writing to
// a block device. The block device is opened lazily on
// the first WriteRequest and kept open across all
// writes. After each batch, acknowledged request IDs
// are sent back to the source.
func (s *RBDDataServer) Sync(
	stream grpc.BidiStreamingServer[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
) (err error) {
	var file *os.File

	defer func() {
		if file != nil {
			if serr := file.Sync(); serr != nil && err == nil {
				err = fmt.Errorf(
					"failed to sync block device "+
						"%s: %w",
					s.devicePath, serr,
				)
			}
			if cerr := file.Close(); cerr != nil && err == nil {
				err = fmt.Errorf(
					"failed to close block device "+
						"%s: %w",
					s.devicePath, cerr,
				)
			}
		}
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		switch op := req.Operation.(type) {
		case *apiv1.SyncRequest_Write:
			if file == nil {
				file, err = os.OpenFile(
					s.devicePath, os.O_RDWR, 0,
				)
				if err != nil {
					return fmt.Errorf(
						"failed to open block "+
							"device %s: %w",
						s.devicePath, err,
					)
				}
			}

			if err := s.writeBlocks(
				file, op.Write,
			); err != nil {
				return err
			}

			ackIDs := collectRequestIDs(
				op.Write.Blocks,
			)
			if len(ackIDs) > 0 {
				if err := stream.Send(
					&apiv1.SyncResponse{
						AcknowledgedIds: ackIDs,
					},
				); err != nil {
					return err
				}
			}

		default:
			return fmt.Errorf(
				"unknown operation type in " +
					"sync request",
			)
		}
	}
}

// collectRequestIDs extracts request IDs from a
// slice of ChangedBlocks. All IDs are included
// (reqID 0 is valid — it's the first block).
func collectRequestIDs(
	blocks []*apiv1.ChangedBlock,
) []uint64 {
	ids := make([]uint64, 0, len(blocks))
	for _, b := range blocks {
		ids = append(ids, b.RequestId)
	}
	return ids
}

// RBDCommitServer implements CommitServiceServer
// for block devices. Commit syncs the block device.
type RBDCommitServer struct {
	apiv1.UnimplementedCommitServiceServer
	logger     logr.Logger
	devicePath string
}

// Commit handles a bidi-streaming RPC for committing
// block device writes. Each CommitRequest triggers an
// fsync on the block device.
func (s *RBDCommitServer) Commit(
	stream grpc.BidiStreamingServer[
		apiv1.CommitRequest, apiv1.CommitResponse,
	],
) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		file, err := os.OpenFile(
			s.devicePath, os.O_RDWR, 0,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to open block device "+
					"%s for commit: %w",
				s.devicePath, err,
			)
		}

		paths := make(
			[]string, 0, len(req.Entries),
		)
		for _, entry := range req.Entries {
			if err := file.Sync(); err != nil {
				_ = file.Close()
				return fmt.Errorf(
					"failed to sync block device "+
						"%s: %w",
					s.devicePath, err,
				)
			}
			s.logger.Info(
				"Committed block device writes",
				"path", entry.Path,
			)
			paths = append(paths, entry.Path)
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf(
				"failed to close block device "+
					"%s: %w",
				s.devicePath, err,
			)
		}

		if err := stream.Send(
			&apiv1.CommitResponse{
				Paths: paths,
			},
		); err != nil {
			return err
		}
	}
}

// writeBlocks writes a batch of changed blocks to
// the block device.
func (s *RBDDataServer) writeBlocks(
	file *os.File, req *apiv1.WriteRequest,
) error {
	s.logger.Info(
		"Writing blocks to device",
		"block_count", len(req.Blocks),
	)

	for i, block := range req.Blocks {
		if block.IsZero {
			zeros := make([]byte, block.Length)
			if _, err := file.WriteAt(
				zeros, int64(block.Offset), //nolint:gosec // G115: value within safe range
			); err != nil {
				s.logger.Error(
					err, "Failed to write zeros",
					"offset", block.Offset,
					"length", block.Length,
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write zeros at "+
						"offset %d: %w",
					block.Offset, err,
				)
			}
			s.logger.V(1).Info(
				"Wrote zero block",
				"offset", block.Offset,
				"length", block.Length,
			)
		} else {
			writeData := block.Data
			if block.Compression == apiv1.CompressionAlgo_COMPRESSION_LZ4 {
				decompressed := make([]byte, block.Length)
				n, err := lz4.UncompressBlock(block.Data, decompressed)
				if err != nil {
					s.logger.Error(err, "Failed to decompress LZ4",
						"offset", block.Offset, "block_index", i)
					return fmt.Errorf("lz4 decompress at offset %d: %w", block.Offset, err)
				}
				writeData = decompressed[:n]
			}

			if _, err := file.WriteAt(
				writeData, int64(block.Offset), //nolint:gosec // G115: value within safe range
			); err != nil {
				s.logger.Error(
					err, "Failed to write data",
					"offset", block.Offset,
					"length", len(writeData),
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write data at "+
						"offset %d: %w",
					block.Offset, err,
				)
			}
			s.logger.V(1).Info(
				"Wrote data block",
				"offset", block.Offset,
				"length", len(writeData),
				"compressed", block.Compression != apiv1.CompressionAlgo_COMPRESSION_NONE,
			)
		}
	}

	return nil
}

// Delete is a no-op for block devices.
func (s *RBDDataServer) Delete(
	_ context.Context, _ *apiv1.DeleteRequest,
) (*apiv1.DeleteResponse, error) {
	return &apiv1.DeleteResponse{}, nil
}
