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

	"github.com/RamenDR/ceph-volsync-plugin/internal/constant"
	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
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
	cache := NewWriteCache(constant.DataMountPath)
	defer func() { _ = cache.Close() }()

	dataServer := &DataServer{
		logger: w.Logger,
		cache:  cache,
	}
	hashServer := &CephFSHashServer{
		logger: w.Logger,
		cache:  cache,
	}
	commitServer := &CephFSCommitServer{
		logger: w.Logger,
		cache:  cache,
	}

	return w.RunWithHashAndCommit(
		ctx, dataServer, hashServer, commitServer,
	)
}

// DataServer implements the DataService gRPC server
// for CephFS file-based writes.
type DataServer struct {
	apiv1.UnimplementedDataServiceServer
	logger logr.Logger
	cache  *FileCache
}

// Sync handles a bidi-streaming RPC that processes
// one file at a time. After writing blocks, it sends
// back acknowledged request IDs. File commits are
// handled by the separate CommitService.
func (s *DataServer) Sync(
	stream grpc.BidiStreamingServer[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
) (err error) {
	var file *os.File
	var curPath string

	defer func() {
		if curPath != "" {
			if serr := s.cache.SyncAndRelease(
				curPath,
			); serr != nil && err == nil {
				err = serr
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
				curPath = op.Write.Path
				totalSize := int64(0)
				if len(op.Write.Blocks) > 0 {
					totalSize = int64( //nolint:gosec // G115
						op.Write.Blocks[0].TotalSize,
					)
				}
				file, err = s.cache.Acquire(
					curPath, totalSize,
				)
				if err != nil {
					return err
				}
			}

			if err := s.writeBlocks(
				file, curPath, op.Write,
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

// CephFSCommitServer implements CommitServiceServer
// for CephFS file-based writes.
type CephFSCommitServer struct {
	apiv1.UnimplementedCommitServiceServer
	logger logr.Logger
	cache  *FileCache
}

// Commit handles a bidi-streaming RPC for committing
// written files. Each CommitRequest triggers fsync
// and release of cached file handles.
func (s *CephFSCommitServer) Commit(
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

		paths := make(
			[]string, 0, len(req.Entries),
		)
		for _, entry := range req.Entries {
			s.logger.Info(
				"Committing file",
				"path", entry.Path,
			)
			if err := s.cache.SyncAndRelease(
				entry.Path,
			); err != nil {
				return err
			}
			s.logger.Info(
				"Successfully committed file",
				"path", entry.Path,
			)
			paths = append(paths, entry.Path)
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
		constant.DataMountPath, cleanPath,
	), nil
}

// writeBlocks writes a batch of changed blocks to
// the file using WriteAt for concurrent safety.
func (s *DataServer) writeBlocks(
	file *os.File, fullPath string,
	req *apiv1.WriteRequest,
) error {
	s.logger.Info(
		"Writing blocks",
		"path", req.Path,
		"block_count", len(req.Blocks),
	)

	for i, block := range req.Blocks {
		offset := int64(block.Offset) //nolint:gosec // G115: value within safe range
		if block.IsZero {
			zeros := make([]byte, block.Length)
			if _, err := file.WriteAt(
				zeros, offset,
			); err != nil {
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
			if _, err := file.WriteAt(
				block.Data, offset,
			); err != nil {
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
