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

package rbd

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// HashServer implements HashHandler for RBD
// block device hash comparison.
type HashServer struct {
	logger     logr.Logger
	devicePath string
}

// CompareHashes handles a bidi stream of hash
// comparison requests. For each batch it reads
// blocks from the local device, computes SHA-256,
// and returns mismatched request IDs.
func (s *HashServer) CompareHashes(
	stream grpc.BidiStreamingServer[
		apiv1.HashRequest,
		apiv1.HashResponse,
	],
) error {
	file, err := os.Open(s.devicePath)
	if err != nil {
		return fmt.Errorf(
			"failed to open device %s: %w",
			s.devicePath, err,
		)
	}
	defer func() { _ = file.Close() }()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp := &apiv1.HashResponse{}
		for _, bh := range req.Hashes {
			data := make([]byte, bh.Length)
			n, readErr := file.ReadAt(
				data, int64(bh.Offset), //nolint:gosec // G115
			)
			if readErr != nil && readErr != io.EOF {
				return fmt.Errorf(
					"pread at offset %d: %w",
					bh.Offset, readErr,
				)
			}

			localHash := sha256.Sum256(data[:n])
			if len(bh.Sha256) != 32 ||
				localHash != [32]byte(bh.Sha256) {
				resp.MismatchedIds = append(
					resp.MismatchedIds,
					bh.RequestId,
				)
			}
		}

		s.logger.V(1).Info(
			"Hash comparison complete",
			"total", len(req.Hashes),
			"mismatched", len(resp.MismatchedIds),
		)
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
