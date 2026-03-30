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
	"io"
	"os"
	"testing"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// mockHashBidiStream implements a single-exchange
// bidi stream for testing.
type mockHashBidiStream struct {
	grpc.BidiStreamingServer[
		apiv1.HashRequest,
		apiv1.HashResponse,
	]
	req  *apiv1.HashRequest
	resp *apiv1.HashResponse
	done bool
}

func (m *mockHashBidiStream) Recv() (
	*apiv1.HashRequest, error,
) {
	if m.done {
		return nil, io.EOF
	}
	m.done = true
	return m.req, nil
}

func (m *mockHashBidiStream) Send(
	resp *apiv1.HashResponse,
) error {
	m.resp = resp
	return nil
}

func TestHashServer_AllMatch(t *testing.T) {
	f, err := os.CreateTemp("", "hashtest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(f.Name()) }()

	data := []byte("0123456789ABCDEF")
	_, _ = f.Write(data)
	_ = f.Close()

	hash := sha256.Sum256(data)
	srv := &HashServer{
		logger:     logr.Discard(),
		devicePath: f.Name(),
	}

	stream := &mockHashBidiStream{
		req: &apiv1.HashRequest{
			Hashes: []*apiv1.BlockHash{
				{
					RequestId: 0,
					Offset:    0,
					Length:    16,
					Sha256:    hash[:],
				},
			},
		},
	}

	if err := srv.CompareHashes(stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.resp.MismatchedIds) != 0 {
		t.Fatalf(
			"expected 0 mismatches, got %d",
			len(stream.resp.MismatchedIds),
		)
	}
}

func TestHashServer_Mismatch(t *testing.T) {
	f, err := os.CreateTemp("", "hashtest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(f.Name()) }()

	data := []byte("0123456789ABCDEF")
	_, _ = f.Write(data)
	_ = f.Close()

	wrongHash := sha256.Sum256([]byte("wrong"))
	srv := &HashServer{
		logger:     logr.Discard(),
		devicePath: f.Name(),
	}

	stream := &mockHashBidiStream{
		req: &apiv1.HashRequest{
			Hashes: []*apiv1.BlockHash{
				{
					RequestId: 0,
					Offset:    0,
					Length:    16,
					Sha256:    wrongHash[:],
				},
			},
		},
	}

	if err := srv.CompareHashes(stream); err != nil {
		t.Fatal(err)
	}
	if len(stream.resp.MismatchedIds) != 1 {
		t.Fatalf(
			"expected 1 mismatch, got %d",
			len(stream.resp.MismatchedIds),
		)
	}
}
