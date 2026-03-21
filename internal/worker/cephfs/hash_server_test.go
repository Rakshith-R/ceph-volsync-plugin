package cephfs

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func TestCephFSHashServer_Mismatch(t *testing.T) {
	dir := t.TempDir()
	content := []byte("block content here")
	path := filepath.Join(dir, "file.bin")
	_ = os.WriteFile(path, content, 0600)

	srv := &CephFSHashServer{
		logger:  logr.Discard(),
		baseDir: dir,
	}

	wrongHash := [32]byte{0xFF}
	req := &apiv1.HashBatchRequest{
		Hashes: []*apiv1.BlockHash{
			{
				RequestId: 42,
				FilePath:  "file.bin",
				Offset:    0,
				Length:    uint64(len(content)),
				Sha256:    wrongHash[:],
			},
		},
	}

	resp, err := srv.CompareHashes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.MismatchedIds) != 1 || resp.MismatchedIds[0] != 42 {
		t.Errorf("expected reqID 42 mismatched")
	}
}

func TestCephFSHashServer_Match(t *testing.T) {
	dir := t.TempDir()
	content := []byte("block content here")
	path := filepath.Join(dir, "file.bin")
	_ = os.WriteFile(path, content, 0600)

	srv := &CephFSHashServer{
		logger:  logr.Discard(),
		baseDir: dir,
	}

	h := sha256.Sum256(content)
	req := &apiv1.HashBatchRequest{
		Hashes: []*apiv1.BlockHash{
			{
				RequestId: 7,
				FilePath:  "file.bin",
				Offset:    0,
				Length:    uint64(len(content)),
				Sha256:    h[:],
			},
		},
	}

	resp, err := srv.CompareHashes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.MismatchedIds) != 0 {
		t.Errorf("expected no mismatches for identical hash")
	}
}

func TestCephFSHashServer_MissingFile(t *testing.T) {
	dir := t.TempDir()
	srv := &CephFSHashServer{
		logger:  logr.Discard(),
		baseDir: dir,
	}
	req := &apiv1.HashBatchRequest{
		Hashes: []*apiv1.BlockHash{
			{RequestId: 1, FilePath: "noexist.bin", Offset: 0, Length: 100},
		},
	}
	resp, err := srv.CompareHashes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.MismatchedIds) != 1 {
		t.Error("missing file should be a mismatch")
	}
}
