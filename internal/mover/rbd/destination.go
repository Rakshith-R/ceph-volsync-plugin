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
	"net"
	"os"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/version/v1"
)

// VersionServer implements the VersionService gRPC server.
type VersionServer struct {
	versionv1.UnimplementedVersionServiceServer
	version string
}

// GetVersion returns version information.
func (s *VersionServer) GetVersion(
	ctx context.Context,
	req *versionv1.GetVersionRequest,
) (*versionv1.GetVersionResponse, error) {
	return &versionv1.GetVersionResponse{
		Version: s.version,
	}, nil
}

// RBDDataServer implements DataService for block devices.
type RBDDataServer struct {
	apiv1.UnimplementedDataServiceServer
	logger     logr.Logger
	devicePath string
}

// Sync handles a client-streaming RPC for writing to a block device.
// The block device is opened lazily on the first WriteRequest and kept
// open across all writes. CommitRequests sync but do not close the device.
func (s *RBDDataServer) Sync(
	stream grpc.ClientStreamingServer[apiv1.SyncRequest, apiv1.SyncResponse],
) error {
	var file *os.File

	defer func() {
		if file != nil {
			file.Sync()
			file.Close()
		}
	}()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&apiv1.SyncResponse{})
		}
		if err != nil {
			return err
		}

		switch op := req.Operation.(type) {
		case *apiv1.SyncRequest_Write:
			// Open the block device lazily on first write.
			if file == nil {
				file, err = os.OpenFile(
					s.devicePath, os.O_RDWR, 0,
				)
				if err != nil {
					return fmt.Errorf(
						"failed to open block device %s: %w",
						s.devicePath, err,
					)
				}
			}

			if err := s.writeBlocks(file, op.Write); err != nil {
				return err
			}

		case *apiv1.SyncRequest_Commit:
			// Sync the device but keep it open for subsequent writes.
			if file != nil {
				s.logger.Info(
					"Committing block device writes",
					"path", s.devicePath,
				)
				if err := file.Sync(); err != nil {
					return fmt.Errorf(
						"failed to sync block device %s: %w",
						s.devicePath, err,
					)
				}
				s.logger.Info(
					"Successfully committed block device writes",
					"path", s.devicePath,
				)
			}

		default:
			return fmt.Errorf("unknown operation type in sync request")
		}
	}
}

// writeBlocks writes a batch of changed blocks to the block device.
func (s *RBDDataServer) writeBlocks(
	file *os.File,
	req *apiv1.WriteRequest,
) error {
	s.logger.Info(
		"Writing blocks to device",
		"block_count", len(req.Blocks),
	)

	for i, block := range req.Blocks {
		if block.IsZero {
			zeros := make([]byte, block.Length)
			if _, err := file.WriteAt(
				zeros, int64(block.Offset),
			); err != nil {
				s.logger.Error(
					err, "Failed to write zeros",
					"offset", block.Offset,
					"length", block.Length,
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write zeros at offset %d: %w",
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
				block.Data, int64(block.Offset),
			); err != nil {
				s.logger.Error(
					err, "Failed to write data",
					"offset", block.Offset,
					"length", len(block.Data),
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write data at offset %d: %w",
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

// Delete is a no-op for block devices. Returns an empty response.
func (s *RBDDataServer) Delete(
	ctx context.Context,
	req *apiv1.DeleteRequest,
) (*apiv1.DeleteResponse, error) {
	return &apiv1.DeleteResponse{}, nil
}

// DoneServer implements the DoneService gRPC server.
type DoneServer struct {
	apiv1.UnimplementedDoneServiceServer
	shutdownChan chan struct{}
}

// Done signals completion and triggers graceful shutdown.
func (s *DoneServer) Done(
	ctx context.Context,
	req *apiv1.DoneRequest,
) (*apiv1.DoneResponse, error) {
	go func() {
		select {
		case s.shutdownChan <- struct{}{}:
		default:
		}
	}()

	return &apiv1.DoneResponse{}, nil
}

// DestinationConfig holds configuration for the RBD destination worker.
type DestinationConfig struct {
	ServerPort string
}

// DestinationWorker represents an RBD destination worker instance.
type DestinationWorker struct {
	logger logr.Logger
	config DestinationConfig
}

// NewDestinationWorker creates a new RBD destination worker.
func NewDestinationWorker(
	logger logr.Logger,
	config DestinationConfig,
) *DestinationWorker {
	return &DestinationWorker{
		logger: logger.WithName("rbd-destination-worker"),
		config: config,
	}
}

// Run starts the RBD destination worker.
func (w *DestinationWorker) Run(ctx context.Context) error {
	w.logger.Info("Starting RBD destination worker")

	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(maxGRPCMessageSize),
	)

	shutdownChan := make(chan struct{}, 1)

	versionServer := &VersionServer{
		version: "v1.0.0",
	}

	dataServer := &RBDDataServer{
		logger:     w.logger,
		devicePath: devicePath,
	}

	doneServer := &DoneServer{
		shutdownChan: shutdownChan,
	}

	versionv1.RegisterVersionServiceServer(server, versionServer)
	apiv1.RegisterDataServiceServer(server, dataServer)
	apiv1.RegisterDoneServiceServer(server, doneServer)

	lis, err := net.Listen("tcp", ":"+w.config.ServerPort)
	if err != nil {
		return fmt.Errorf(
			"failed to listen on port %s: %w",
			w.config.ServerPort, err,
		)
	}

	w.logger.Info(
		"gRPC server listening", "port", w.config.ServerPort,
	)

	serverErr := make(chan error, 1)
	go func() {
		if err := server.Serve(lis); err != nil {
			serverErr <- fmt.Errorf("gRPC server failed: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		w.logger.Info(
			"RBD destination worker shutting down " +
				"due to context cancellation",
		)
		server.GracefulStop()
		return ctx.Err()
	case err := <-serverErr:
		return err
	case <-shutdownChan:
		w.logger.Info(
			"RBD destination worker shutting down " +
				"after Done request",
		)
		server.GracefulStop()
		return nil
	}
}
