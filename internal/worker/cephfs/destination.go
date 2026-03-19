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

package cephfs

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// DestinationWorker represents a CephFS destination
// worker instance.
type DestinationWorker struct {
	common.BaseDestinationWorker
}

// NewDestinationWorker creates a new CephFS
// destination worker.
func NewDestinationWorker(
	logger logr.Logger,
	config common.DestinationConfig,
) *DestinationWorker {
	return &DestinationWorker{
		BaseDestinationWorker: common.BaseDestinationWorker{
			Logger: logger.WithName(
				"cephfs-destination-worker",
			),
			Config: config,
		},
	}
}

// Run starts the CephFS destination worker.
func (w *DestinationWorker) Run(
	ctx context.Context,
) error {
	dataServer := &DataServer{
		logger: w.Logger,
	}

	return w.BaseDestinationWorker.Run(
		ctx, dataServer,
	)
}

// DataServer implements the DataService gRPC server
// for CephFS file-based writes.
type DataServer struct {
	apiv1.UnimplementedDataServiceServer
	logger logr.Logger
}

// Sync handles a client-streaming RPC that processes
// one file at a time. Each stream is reusable: after a
// CommitRequest the stream resets and accepts writes for
// the next file.
func (s *DataServer) Sync(
	stream grpc.ClientStreamingServer[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
) (err error) {
	var file *os.File
	var fullPath string

	defer func() {
		if file != nil {
			if serr := file.Sync(); serr != nil && err == nil {
				err = fmt.Errorf(
					"failed to sync file: %w", serr,
				)
			}
			if cerr := file.Close(); cerr != nil && err == nil {
				err = fmt.Errorf(
					"failed to close file: %w", cerr,
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
				fullPath, err = sanitizePath(
					op.Write.Path,
				)
				if err != nil {
					return err
				}

				dir := filepath.Dir(fullPath)
				if err := os.MkdirAll(dir, 0755); err != nil { //nolint:gosec // G301: rsync daemon needs world-readable dirs
					return fmt.Errorf(
						"failed to create parent "+
							"directory %s: %w",
						dir, err,
					)
				}

				file, err = os.OpenFile( //nolint:gosec // G304: path constructed from validated input
					fullPath,
					os.O_RDWR|os.O_CREATE, 0644,
				)
				if err != nil {
					return fmt.Errorf(
						"failed to open file %s: %w",
						fullPath, err,
					)
				}
			}

			if err := s.writeBlocks(
				file, fullPath, op.Write,
			); err != nil {
				return err
			}

		case *apiv1.SyncRequest_Commit:
			if file != nil {
				s.logger.Info(
					"Committing file",
					"path", fullPath,
				)
				if err := file.Sync(); err != nil {
					return fmt.Errorf(
						"failed to sync file %s: %w",
						fullPath, err,
					)
				}
				if err := file.Close(); err != nil {
					return fmt.Errorf(
						"failed to close file %s: %w",
						fullPath, err,
					)
				}
				s.logger.Info(
					"Successfully committed file",
					"path", fullPath,
				)
				file = nil
				fullPath = ""
			}

		default:
			return fmt.Errorf(
				"unknown operation type in sync request",
			)
		}
	}
}

// sanitizePath validates and resolves a relative path
// to a full /data path.
func sanitizePath(relPath string) (string, error) {
	if relPath == "" {
		return "", fmt.Errorf("path cannot be empty")
	}

	cleanPath := filepath.Clean(relPath)
	if strings.Contains(cleanPath, "..") {
		return "", fmt.Errorf(
			"invalid path: path traversal not allowed",
		)
	}

	return filepath.Join(
		common.DataMountPath, cleanPath,
	), nil
}

// writeBlocks writes a batch of changed blocks to
// the file.
func (s *DataServer) writeBlocks(
	file *os.File, fullPath string,
	req *apiv1.WriteRequest,
) error {
	s.logger.Info(
		"Writing blocks",
		"path", req.Path,
		"block_count", len(req.Blocks),
	)

	var maxSize int64
	for _, block := range req.Blocks {
		endOffset := int64(block.Offset + block.Length) //nolint:gosec // G115: value within safe range
		if endOffset > maxSize {
			maxSize = endOffset
		}
	}

	fileInfo, err := file.Stat()
	if err != nil {
		s.logger.Error(
			err, "Failed to stat file",
			"path", fullPath,
		)
		return fmt.Errorf(
			"failed to stat file %s: %w",
			fullPath, err,
		)
	}

	if maxSize > fileInfo.Size() {
		if err := file.Truncate(maxSize); err != nil {
			s.logger.Error(
				err, "Failed to truncate file",
				"path", fullPath, "size", maxSize,
			)
			return fmt.Errorf(
				"failed to resize file %s to "+
					"%d bytes: %w",
				fullPath, maxSize, err,
			)
		}
		s.logger.Info(
			"Resized file",
			"path", fullPath,
			"old_size", fileInfo.Size(),
			"new_size", maxSize,
		)
	}

	for i, block := range req.Blocks {
		if _, err := file.Seek(int64(block.Offset), 0); err != nil { //nolint:gosec // G115: value within safe range
			s.logger.Error(
				err, "Failed to seek",
				"path", fullPath,
				"offset", block.Offset,
				"block_index", i,
			)
			return fmt.Errorf(
				"failed to seek to offset %d "+
					"in %s: %w",
				block.Offset, fullPath, err,
			)
		}

		if block.IsZero {
			zeros := make([]byte, block.Length)
			if _, err := file.Write(zeros); err != nil {
				s.logger.Error(
					err, "Failed to write zeros",
					"path", fullPath,
					"offset", block.Offset,
					"length", block.Length,
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write zeros at "+
						"offset %d in %s: %w",
					block.Offset, fullPath, err,
				)
			}
			s.logger.V(1).Info(
				"Wrote zero block",
				"path", fullPath,
				"offset", block.Offset,
				"length", block.Length,
			)
		} else {
			if _, err := file.Write(block.Data); err != nil {
				s.logger.Error(
					err, "Failed to write data",
					"path", fullPath,
					"offset", block.Offset,
					"length", len(block.Data),
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write data at "+
						"offset %d in %s: %w",
					block.Offset, fullPath, err,
				)
			}
			s.logger.V(1).Info(
				"Wrote data block",
				"path", fullPath,
				"offset", block.Offset,
				"length", len(block.Data),
			)
		}
	}

	return nil
}

// deleteParallelism is the maximum number of concurrent
// os.RemoveAll operations.
const deleteParallelism = 16

// Delete handles a unary RPC to delete files or
// directories.
func (s *DataServer) Delete(
	ctx context.Context, req *apiv1.DeleteRequest,
) (*apiv1.DeleteResponse, error) {
	s.logger.Info(
		"Deleting paths", "count", len(req.Paths),
	)

	fullPaths := make([]string, 0, len(req.Paths))
	for _, path := range req.Paths {
		fullPath, err := sanitizePath(path)
		if err != nil {
			return nil, err
		}
		fullPaths = append(fullPaths, fullPath)
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(deleteParallelism)

	for _, fullPath := range fullPaths {
		g.Go(func() error {
			if gctx.Err() != nil {
				return gctx.Err()
			}
			if err := os.RemoveAll(fullPath); err != nil {
				s.logger.Error(
					err, "Failed to delete path",
					"path", fullPath,
				)
				return fmt.Errorf(
					"failed to delete path %s: %w",
					fullPath, err,
				)
			}
			s.logger.V(1).Info(
				"Deleted path", "path", fullPath,
			)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	s.logger.Info(
		"Successfully deleted paths",
		"count", len(req.Paths),
	)
	return &apiv1.DeleteResponse{}, nil
}
