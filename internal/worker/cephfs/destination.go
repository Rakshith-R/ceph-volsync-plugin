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
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-logr/logr"
	"github.com/pierrec/lz4/v4"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
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
func (w *DestinationWorker) Run(ctx context.Context) error {
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

	syncServer := common.NewSyncServer(dataServer, dataServer, hashServer, commitServer)
	return w.BaseDestinationWorker.Run(ctx, syncServer)
}

// DataServer implements WriteHandler and DeleteHandler
// for CephFS file-based writes.
type DataServer struct {
	logger logr.Logger
	cache  *FileCache
}

// Write handles a bidi-streaming RPC that processes
// blocks from one or more files. After writing blocks,
// it sends back acknowledged request IDs. File commits
// are handled by the separate Commit RPC.
func (s *DataServer) Write(
	stream grpc.BidiStreamingServer[
		apiv1.WriteRequest, apiv1.WriteResponse,
	],
) (err error) {
	files := make(map[string]*os.File)

	defer func() {
		for path := range files {
			if serr := s.cache.SyncAndRelease(
				path,
			); serr != nil {
				if err == nil {
					err = serr
				} else {
					s.logger.Error(serr, "SyncAndRelease failed during cleanup", "path", path)
				}
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

		if err := s.writeBlocks(
			files, req,
		); err != nil {
			return err
		}

		ackIDs := collectRequestIDs(req.Blocks)
		if len(ackIDs) > 0 {
			if err := stream.Send(
				&apiv1.WriteResponse{
					AcknowledgedIds: ackIDs,
				},
			); err != nil {
				return err
			}
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

// CephFSCommitServer implements CommitHandler
// for CephFS file-based writes.
type CephFSCommitServer struct {
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

// writeBlocks writes a batch of changed blocks,
// looking up or acquiring file handles per block.
func (s *DataServer) writeBlocks(
	files map[string]*os.File,
	req *apiv1.WriteRequest,
) error {
	if len(req.Blocks) == 0 {
		return nil
	}

	s.logger.Info(
		"Writing blocks",
		"path", req.Blocks[0].FilePath,
		"block_count", len(req.Blocks),
	)

	filePath := req.Blocks[0].FilePath
	file, ok := files[filePath]
	if !ok {
		var err error
		file, err = s.cache.Acquire(
			filePath,
			int64(req.Blocks[0].TotalSize), //nolint:gosec // G115
		)
		if err != nil {
			return err
		}
		files[filePath] = file
	}

	for i, block := range req.Blocks {
		offset := int64(block.Offset) //nolint:gosec // G115: value within safe range
		if block.IsZero {
			zeros := make([]byte, block.Length)
			if _, err := file.WriteAt(
				zeros, offset,
			); err != nil {
				s.logger.Error(
					err, "Failed to write zeros",
					"path", filePath,
					"offset", block.Offset,
					"length", block.Length,
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write zeros at "+
						"offset %d in %s: %w",
					block.Offset, filePath, err,
				)
			}
			s.logger.V(1).Info(
				"Wrote zero block",
				"path", filePath,
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
						"path", filePath, "offset", block.Offset,
						"block_index", i)
					return fmt.Errorf("lz4 decompress at offset %d in %s: %w",
						block.Offset, filePath, err)
				}
				writeData = decompressed[:n]
				if n != int(block.Length) { //nolint:gosec // block.Length is bounded by pipeline chunk size
					return fmt.Errorf(
						"lz4 decompressed size mismatch at offset %d in %s: got %d, expected %d",
						block.Offset, filePath, n, block.Length,
					)
				}
			}

			if _, err := file.WriteAt(
				writeData, offset,
			); err != nil {
				s.logger.Error(
					err, "Failed to write data",
					"path", filePath,
					"offset", block.Offset,
					"length", len(writeData),
					"block_index", i,
				)
				return fmt.Errorf(
					"failed to write data at "+
						"offset %d in %s: %w",
					block.Offset, filePath, err,
				)
			}
			s.logger.V(1).Info(
				"Wrote data block",
				"path", filePath,
				"offset", block.Offset,
				"length", len(writeData),
				"compressed", block.Compression != apiv1.CompressionAlgo_COMPRESSION_NONE,
			)
		}
	}

	return nil
}

// deleteParallelism is the maximum number of concurrent
// os.RemoveAll operations.
const deleteParallelism = 16

// Delete handles a bidi-streaming RPC to delete files
// or directories.
func (s *DataServer) Delete(
	stream grpc.BidiStreamingServer[
		apiv1.DeleteRequest, apiv1.DeleteResponse,
	],
) error {
	ctx := stream.Context()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		s.logger.Info(
			"Deleting paths",
			"count", len(req.Paths),
		)

		fullPaths := make(
			[]string, 0, len(req.Paths),
		)
		for _, path := range req.Paths {
			fullPath, err := sanitizePath(path)
			if err != nil {
				return err
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
				if err := os.RemoveAll(
					fullPath,
				); err != nil {
					s.logger.Error(
						err,
						"Failed to delete path",
						"path", fullPath,
					)
					return fmt.Errorf(
						"failed to delete "+
							"path %s: %w",
						fullPath, err,
					)
				}
				s.logger.V(1).Info(
					"Deleted path",
					"path", fullPath,
				)
				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return err
		}

		s.logger.Info(
			"Successfully deleted paths",
			"count", len(req.Paths),
		)
		if err := stream.Send(
			&apiv1.DeleteResponse{},
		); err != nil {
			return err
		}
	}
}
