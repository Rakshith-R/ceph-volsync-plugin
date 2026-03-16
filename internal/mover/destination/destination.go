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

package destination

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/version/v1"
)

// Config holds configuration for the destination worker
type Config struct {
	ServerPort string
}

// VersionServer implements the VersionService gRPC server
type VersionServer struct {
	versionv1.UnimplementedVersionServiceServer
	version string
}

// GetVersion returns version information
func (s *VersionServer) GetVersion(
	ctx context.Context, req *versionv1.GetVersionRequest,
) (*versionv1.GetVersionResponse, error) {
	response := &versionv1.GetVersionResponse{
		Version: s.version,
	}
	return response, nil
}

// DataServer implements the DataService gRPC server
type DataServer struct {
	apiv1.UnimplementedDataServiceServer
	logger logr.Logger
}

// Sync handles a client-streaming RPC that processes one file at a time.
// Each stream is reusable: after a CommitRequest the stream resets and
// accepts writes for the next file.
func (s *DataServer) Sync(
	stream grpc.ClientStreamingServer[apiv1.SyncRequest, apiv1.SyncResponse],
) error {
	var file *os.File
	var fullPath string

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
			// First write for a new file: open it
			if file == nil {
				fullPath, err = sanitizePath(op.Write.Path)
				if err != nil {
					return err
				}

				dir := filepath.Dir(fullPath)
				if err := os.MkdirAll(dir, 0755); err != nil {
					return fmt.Errorf(
						"failed to create parent directory %s: %w", dir, err,
					)
				}

				file, err = os.OpenFile(fullPath, os.O_RDWR|os.O_CREATE, 0644)
				if err != nil {
					return fmt.Errorf("failed to open file %s: %w", fullPath, err)
				}
			}

			if err := s.writeBlocks(file, fullPath, op.Write); err != nil {
				return err
			}

		case *apiv1.SyncRequest_Commit:
			// Commit: sync + close current file, reset for next file
			if file != nil {
				s.logger.Info("Committing file", "path", fullPath)
				if err := file.Sync(); err != nil {
					return fmt.Errorf("failed to sync file %s: %w", fullPath, err)
				}
				if err := file.Close(); err != nil {
					return fmt.Errorf("failed to close file %s: %w", fullPath, err)
				}
				s.logger.Info("Successfully committed file", "path", fullPath)
				file = nil
				fullPath = ""
			}

		default:
			return fmt.Errorf("unknown operation type in sync request")
		}
	}
}

// sanitizePath validates and resolves a relative path to a full /data path.
func sanitizePath(relPath string) (string, error) {
	if relPath == "" {
		return "", fmt.Errorf("path cannot be empty")
	}

	cleanPath := filepath.Clean(relPath)
	if strings.Contains(cleanPath, "..") {
		return "", fmt.Errorf("invalid path: path traversal not allowed")
	}

	return filepath.Join("/data", cleanPath), nil
}

// writeBlocks writes a batch of changed blocks to the file.
func (s *DataServer) writeBlocks(
	file *os.File, fullPath string, req *apiv1.WriteRequest,
) error {
	s.logger.Info("Writing blocks", "path", req.Path, "block_count", len(req.Blocks))

	// Calculate the maximum file size needed based on all blocks
	var maxSize int64
	for _, block := range req.Blocks {
		endOffset := int64(block.Offset + block.Length)
		if endOffset > maxSize {
			maxSize = endOffset
		}
	}

	// Ensure file is large enough for all writes
	fileInfo, err := file.Stat()
	if err != nil {
		s.logger.Error(err, "Failed to stat file", "path", fullPath)
		return fmt.Errorf("failed to stat file %s: %w", fullPath, err)
	}

	if maxSize > fileInfo.Size() {
		if err := file.Truncate(maxSize); err != nil {
			s.logger.Error(err, "Failed to truncate file",
				"path", fullPath, "size", maxSize,
			)
			return fmt.Errorf(
				"failed to resize file %s to %d bytes: %w", fullPath, maxSize, err,
			)
		}
		s.logger.Info("Resized file",
			"path", fullPath,
			"old_size", fileInfo.Size(),
			"new_size", maxSize,
		)
	}

	// Process each block
	for i, block := range req.Blocks {
		if _, err := file.Seek(int64(block.Offset), 0); err != nil {
			s.logger.Error(err, "Failed to seek",
				"path", fullPath, "offset", block.Offset, "block_index", i,
			)
			return fmt.Errorf(
				"failed to seek to offset %d in %s: %w", block.Offset, fullPath, err,
			)
		}

		if block.IsZero {
			zeros := make([]byte, block.Length)
			if _, err := file.Write(zeros); err != nil {
				s.logger.Error(err, "Failed to write zeros",
					"path", fullPath,
					"offset", block.Offset,
					"length", block.Length,
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write zeros at offset %d in %s: %w",
					block.Offset, fullPath, err,
				)
			}
			s.logger.V(1).Info("Wrote zero block",
				"path", fullPath, "offset", block.Offset, "length", block.Length,
			)
		} else {
			if _, err := file.Write(block.Data); err != nil {
				s.logger.Error(err, "Failed to write data",
					"path", fullPath,
					"offset", block.Offset,
					"length", len(block.Data),
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write data at offset %d in %s: %w",
					block.Offset, fullPath, err,
				)
			}
			s.logger.V(1).Info("Wrote data block",
				"path", fullPath, "offset", block.Offset, "length", len(block.Data),
			)
		}
	}

	return nil
}

// deleteParallelism is the maximum number of concurrent os.RemoveAll operations.
const deleteParallelism = 16

// Delete handles a unary RPC to delete files or directories.
func (s *DataServer) Delete(
	ctx context.Context, req *apiv1.DeleteRequest,
) (*apiv1.DeleteResponse, error) {
	s.logger.Info("Deleting paths", "count", len(req.Paths))

	// Phase 1: Pre-validate all paths (fail fast before any I/O).
	fullPaths := make([]string, 0, len(req.Paths))
	for _, path := range req.Paths {
		fullPath, err := sanitizePath(path)
		if err != nil {
			return nil, err
		}
		fullPaths = append(fullPaths, fullPath)
	}

	// Phase 2: Delete from disk in parallel with bounded concurrency.
	// Use WithContext so that the first failure cancels remaining work.
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(deleteParallelism)

	for _, fullPath := range fullPaths {
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			if err := os.RemoveAll(fullPath); err != nil {
				s.logger.Error(err, "Failed to delete path", "path", fullPath)
				return fmt.Errorf("failed to delete path %s: %w", fullPath, err)
			}
			s.logger.V(1).Info("Deleted path", "path", fullPath)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	s.logger.Info("Successfully deleted paths", "count", len(req.Paths))
	return &apiv1.DeleteResponse{}, nil
}

// DoneServer implements the DoneService gRPC server
type DoneServer struct {
	apiv1.UnimplementedDoneServiceServer
	shutdownChan chan struct{}
}

// Done signals completion and triggers graceful shutdown
func (s *DoneServer) Done(
	ctx context.Context, req *apiv1.DoneRequest,
) (*apiv1.DoneResponse, error) {
	// Signal shutdown after responding to the request
	go func() {
		select {
		case s.shutdownChan <- struct{}{}:
		default:
			// Channel already has a signal or is closed
		}
	}()

	return &apiv1.DoneResponse{}, nil
}

// Worker represents a destination worker instance
type Worker struct {
	logger logr.Logger
	config Config
}

// NewWorker creates a new destination worker
func NewWorker(logger logr.Logger, config Config) *Worker {
	return &Worker{
		logger: logger.WithName("destination-worker"),
		config: config,
	}
}

// Run starts the destination worker
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("Starting destination worker")

	// Create gRPC server
	server := grpc.NewServer()

	// Create shutdown channel for coordinating graceful shutdown
	shutdownChan := make(chan struct{}, 1)

	// Create version service
	versionServer := &VersionServer{
		version: "v1.0.0", // TODO: Get from build info or config
	}

	// Create data service
	dataServer := &DataServer{
		logger: w.logger,
	}

	// Create done service
	doneServer := &DoneServer{
		shutdownChan: shutdownChan,
	}

	// Register services
	versionv1.RegisterVersionServiceServer(server, versionServer)
	apiv1.RegisterDataServiceServer(server, dataServer)
	apiv1.RegisterDoneServiceServer(server, doneServer)

	// Setup listener
	lis, err := net.Listen("tcp", ":"+w.config.ServerPort)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", w.config.ServerPort, err)
	}

	w.logger.Info("gRPC server listening", "port", w.config.ServerPort)

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		if err := server.Serve(lis); err != nil {
			serverErr <- fmt.Errorf("gRPC server failed: %w", err)
		}
	}()

	// Wait for context cancellation, server error, or shutdown signal
	select {
	case <-ctx.Done():
		w.logger.Info("Destination worker shutting down due to context cancellation")
		server.GracefulStop()
		return ctx.Err()
	case err := <-serverErr:
		return err
	case <-shutdownChan:
		w.logger.Info("Destination worker shutting down after Done request")
		server.GracefulStop()
		return nil
	}
}
