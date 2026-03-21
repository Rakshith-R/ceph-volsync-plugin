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

package common

import (
	"fmt"
	"io"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// BlockEntry represents a single changed block to be
// streamed to the destination via gRPC. The ReqID
// field groups blocks belonging to the same commit
// unit (one file for CephFS, the block device for
// RBD). When the ReqID changes, StreamBlocks sends
// a CommitRequest for the previous unit.
type BlockEntry struct {
	ReqID    uint64
	FilePath string
	Offset   int64
	Length   int64
	IsZero   bool
	Data     []byte // nil when IsZero is true
}

// ReadBlockEntry reads data from reader at the given
// offset and length, detects zero blocks, and returns
// a populated BlockEntry.
func ReadBlockEntry(
	reader io.ReaderAt,
	reqID uint64,
	filePath string,
	offset, length int64,
) (BlockEntry, error) {
	data := make([]byte, length)

	n, err := reader.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return BlockEntry{}, fmt.Errorf(
			"failed to read at offset %d: %w",
			offset, err,
		)
	}

	data = data[:n]
	isZero := IsAllZero(data)

	entry := BlockEntry{
		ReqID:    reqID,
		FilePath: filePath,
		Offset:   offset,
		Length:   length,
		IsZero:   isZero,
	}

	if !isZero {
		entry.Data = data
	}

	return entry, nil
}

// StreamBlocks reads BlockEntry items from blockChan,
// batches them into gRPC WriteRequests, and sends
// CommitRequests when the ReqID changes or when the
// channel closes. Returns nil without sending a
// CommitRequest if no blocks are received.
func StreamBlocks(
	stream grpc.ClientStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
	blockChan <-chan BlockEntry,
	logger logr.Logger,
) error {
	var (
		started      bool
		currentPath  string
		currentReqID uint64

		accumulatedBlocks  []*apiv1.ChangedBlock
		accumulatedPayload int
	)

	flush := func() error {
		if len(accumulatedBlocks) == 0 {
			return nil
		}

		if err := SendBlockWrite(
			stream, currentPath, accumulatedBlocks,
		); err != nil {
			return err
		}

		accumulatedBlocks = nil
		accumulatedPayload = 0

		return nil
	}

	commit := func() error {
		if err := flush(); err != nil {
			return err
		}

		logger.V(1).Info(
			"Committing path",
			"path", currentPath,
		)

		return sendCommit(stream, currentPath)
	}

	for entry := range blockChan {
		if started && entry.ReqID != currentReqID {
			if err := commit(); err != nil {
				return err
			}
		}

		started = true
		currentReqID = entry.ReqID
		currentPath = entry.FilePath

		protoBlock := &apiv1.ChangedBlock{
			Offset: uint64(entry.Offset), //nolint:gosec // G115: offsets are non-negative
			Length: uint64(entry.Length), //nolint:gosec // G115: lengths are non-negative
			IsZero: entry.IsZero,
		}

		if !entry.IsZero {
			protoBlock.Data = entry.Data
			accumulatedPayload += len(entry.Data)
		} else {
			accumulatedPayload += 20
		}

		accumulatedBlocks = append(
			accumulatedBlocks, protoBlock,
		)

		if accumulatedPayload >= WritePayloadMaxSize {
			if err := flush(); err != nil {
				return err
			}

			continue
		}

		if accumulatedPayload >= WritePayloadMinSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if !started {
		return nil
	}

	return commit()
}

// sendCommit sends a CommitRequest on the stream.
// Handles EOF errors by checking the server response.
func sendCommit(
	stream grpc.ClientStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
	path string,
) error {
	if err := stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Commit{
			Commit: &apiv1.CommitRequest{
				Path: path,
			},
		},
	}); err != nil {
		if err == io.EOF {
			if _, recvErr := stream.CloseAndRecv(); recvErr != nil {
				return fmt.Errorf(
					"destination error during "+
						"commit for %s: %w",
					path, recvErr,
				)
			}
		}

		return fmt.Errorf(
			"failed to send commit for %s: %w",
			path, err,
		)
	}

	return nil
}
