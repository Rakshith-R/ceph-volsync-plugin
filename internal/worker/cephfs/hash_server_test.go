package cephfs

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// mockHashBidiStream implements a single-exchange
// bidi stream for testing: one Send, one Recv.
type mockHashBidiStream struct {
	grpc.BidiStreamingServer[
		apiv1.HashBatchRequest,
		apiv1.HashBatchResponse,
	]
	req  *apiv1.HashBatchRequest
	resp *apiv1.HashBatchResponse
	done bool
}

func (m *mockHashBidiStream) Recv() (
	*apiv1.HashBatchRequest, error,
) {
	if m.done {
		return nil, io.EOF
	}
	m.done = true
	return m.req, nil
}

func (m *mockHashBidiStream) Send(
	resp *apiv1.HashBatchResponse,
) error {
	m.resp = resp
	return nil
}

func TestCephFSHashServer_Mismatch(t *testing.T) {
	dir := t.TempDir()
	content := []byte("block content here")
	path := filepath.Join(dir, "file.bin")
	_ = os.WriteFile(path, content, 0600)

	cache := NewReadCache(dir)
	defer func() { _ = cache.Close() }()

	srv := &CephFSHashServer{
		logger: logr.Discard(),
		cache:  cache,
	}

	wrongHash := [32]byte{0xFF}
	stream := &mockHashBidiStream{
		req: &apiv1.HashBatchRequest{
			Hashes: []*apiv1.BlockHash{
				{
					RequestId: 42,
					FilePath:  "file.bin",
					Offset:    0,
					Length:    uint64(len(content)),
					Sha256:    wrongHash[:],
				},
			},
		},
	}

	if err := srv.CompareHashes(stream); err != nil {
		t.Fatal(err)
	}
	if stream.resp == nil {
		t.Fatal("no response received")
	}
	if len(stream.resp.MismatchedIds) != 1 ||
		stream.resp.MismatchedIds[0] != 42 {
		t.Errorf("expected reqID 42 mismatched")
	}
}

func TestCephFSHashServer_Match(t *testing.T) {
	dir := t.TempDir()
	content := []byte("block content here")
	path := filepath.Join(dir, "file.bin")
	_ = os.WriteFile(path, content, 0600)

	cache := NewReadCache(dir)
	defer func() { _ = cache.Close() }()

	srv := &CephFSHashServer{
		logger: logr.Discard(),
		cache:  cache,
	}

	h := sha256.Sum256(content)
	stream := &mockHashBidiStream{
		req: &apiv1.HashBatchRequest{
			Hashes: []*apiv1.BlockHash{
				{
					RequestId: 7,
					FilePath:  "file.bin",
					Offset:    0,
					Length:    uint64(len(content)),
					Sha256:    h[:],
				},
			},
		},
	}

	if err := srv.CompareHashes(stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.resp.MismatchedIds) != 0 {
		t.Errorf(
			"expected no mismatches for identical hash",
		)
	}
}

func TestCephFSHashServer_MissingFile(t *testing.T) {
	dir := t.TempDir()
	cache := NewReadCache(dir)
	defer func() { _ = cache.Close() }()

	srv := &CephFSHashServer{
		logger: logr.Discard(),
		cache:  cache,
	}

	stream := &mockHashBidiStream{
		req: &apiv1.HashBatchRequest{
			Hashes: []*apiv1.BlockHash{
				{
					RequestId: 1,
					FilePath:  "noexist.bin",
					Offset:    0,
					Length:    100,
				},
			},
		},
	}

	if err := srv.CompareHashes(stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.resp.MismatchedIds) != 1 {
		t.Error("missing file should be a mismatch")
	}
}
