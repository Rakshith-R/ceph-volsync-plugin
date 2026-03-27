package cephfs

import (
	"crypto/sha256"
	"io"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// CephFSHashServer implements HashHandler for
// CephFS file-based hash comparison.
// For each requested block it reads the destination
// file at the given offset and compares SHA-256.
// Uses a shared FileCache for concurrent access.
type CephFSHashServer struct {
	logger logr.Logger
	cache  *FileCache
}

// CompareHashes handles a bidi stream of hash
// comparison requests. For each batch received,
// it reads destination file blocks, computes
// SHA-256, and returns mismatched request IDs.
func (s *CephFSHashServer) CompareHashes(
	stream grpc.BidiStreamingServer[
		apiv1.HashRequest,
		apiv1.HashResponse,
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

		resp := &apiv1.HashResponse{}
		for _, bh := range req.Hashes {
			f, err := s.cache.Acquire(
				bh.FilePath,
				int64(bh.TotalSize), //nolint:gosec // G115
			)
			if err != nil {
				resp.MismatchedIds = append(
					resp.MismatchedIds,
					bh.RequestId,
				)
				continue
			}

			data := make([]byte, bh.Length)
			n, readErr := f.ReadAt(
				data, int64(bh.Offset), //nolint:gosec // G115
			)
			// Release immediately after read.
			_ = s.cache.Release(bh.FilePath)

			if readErr != nil && readErr != io.EOF {
				resp.MismatchedIds = append(
					resp.MismatchedIds,
					bh.RequestId,
				)
				continue
			}

			localHash := sha256.Sum256(data[:n])
			if len(bh.Sha256) != 32 ||
				localHash != ([32]byte)(bh.Sha256) {
				resp.MismatchedIds = append(
					resp.MismatchedIds,
					bh.RequestId,
				)
			}
		}

		s.logger.V(1).Info(
			"Hash comparison",
			"total", len(req.Hashes),
			"mismatched", len(resp.MismatchedIds),
		)
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
