package cephfs

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/go-logr/logr"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// CephFSHashServer implements HashServiceServer for
// CephFS file-based hash comparison.
// For each requested block it reads the destination
// file at the given offset and compares SHA-256.
type CephFSHashServer struct {
	apiv1.UnimplementedHashServiceServer
	logger  logr.Logger
	baseDir string
}

// NewCephFSHashServer creates a hash server rooted at
// common.DataMountPath (production use).
func NewCephFSHashServer(logger logr.Logger) *CephFSHashServer {
	return &CephFSHashServer{
		logger:  logger,
		baseDir: common.DataMountPath,
	}
}

// CompareHashes reads destination file blocks,
// computes SHA-256, and returns mismatched req IDs.
func (s *CephFSHashServer) CompareHashes(
	_ context.Context,
	req *apiv1.HashBatchRequest,
) (*apiv1.HashBatchResponse, error) {
	resp := &apiv1.HashBatchResponse{}

	for _, bh := range req.Hashes {
		full := filepath.Join(s.baseDir, bh.FilePath)
		data, err := readAt(
			full, int64(bh.Offset), //nolint:gosec
			int64(bh.Length), //nolint:gosec
		)
		if err != nil {
			// File not present on destination =
			// must send all its blocks.
			resp.MismatchedIds = append(
				resp.MismatchedIds, bh.RequestId,
			)
			continue
		}

		localHash := sha256.Sum256(data)
		if len(bh.Sha256) != 32 ||
			localHash != ([32]byte)(bh.Sha256) {
			resp.MismatchedIds = append(
				resp.MismatchedIds, bh.RequestId,
			)
		}
	}

	s.logger.V(1).Info(
		"Hash comparison",
		"total", len(req.Hashes),
		"mismatched", len(resp.MismatchedIds),
	)
	return resp, nil
}

func readAt(
	path string, offset, length int64,
) ([]byte, error) {
	f, err := os.Open(path) //nolint:gosec // G304
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	data := make([]byte, length)
	n, err := f.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("pread %s at %d: %w", path, offset, err)
	}
	return data[:n], nil
}
