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
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
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
		devicePath: common.DevicePath,
	}
	return w.BaseDestinationWorker.Run(ctx, dataServer)
}

// RBDDataServer implements DataService for block
// devices.
type RBDDataServer struct {
	apiv1.UnimplementedDataServiceServer
	logger     logr.Logger
	devicePath string
}

// Sync handles a client-streaming RPC for writing to
// a block device. The block device is opened lazily on
// the first WriteRequest and kept open across all
// writes. CommitRequests sync but do not close the
// device.
func (s *RBDDataServer) Sync(
	stream grpc.ClientStreamingServer[
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
			return stream.SendAndClose(
				&apiv1.SyncResponse{},
			)
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

		case *apiv1.SyncRequest_Commit:
			if file != nil {
				s.logger.Info(
					"Committing block device writes",
					"path", s.devicePath,
				)
				if err := file.Sync(); err != nil {
					return fmt.Errorf(
						"failed to sync block "+
							"device %s: %w",
						s.devicePath, err,
					)
				}
				s.logger.Info(
					"Successfully committed block "+
						"device writes",
					"path", s.devicePath,
				)
			}

		default:
			return fmt.Errorf(
				"unknown operation type in " +
					"sync request",
			)
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
			if _, err := file.WriteAt(
				block.Data, int64(block.Offset), //nolint:gosec // G115: value within safe range
			); err != nil {
				s.logger.Error(
					err, "Failed to write data",
					"offset", block.Offset,
					"length", len(block.Data),
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
				"length", len(block.Data),
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
