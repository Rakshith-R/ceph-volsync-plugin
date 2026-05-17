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
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// mockWriteBidiStream implements a single-exchange bidi stream
// for Write RPC testing.
type mockWriteBidiStream struct {
	grpc.BidiStreamingServer[
		apiv1.WriteRequest,
		apiv1.WriteResponse,
	]
	req  *apiv1.WriteRequest
	resp *apiv1.WriteResponse
	done bool
}

func (m *mockWriteBidiStream) Recv() (
	*apiv1.WriteRequest, error,
) {
	if m.done {
		return nil, io.EOF
	}
	m.done = true
	return m.req, nil
}

func (m *mockWriteBidiStream) Send(
	resp *apiv1.WriteResponse,
) error {
	m.resp = resp
	return nil
}

// TestWriteBlocks_ZeroBlock verifies that writeBlocks handles
// zero blocks by zeroing the corresponding range in the file.
func TestWriteBlocks_ZeroBlock(t *testing.T) {
	dir := t.TempDir()
	cache := NewWriteCache(dir)
	defer func() { _ = cache.Close() }()

	srv := &DataServer{
		logger: logr.Discard(),
		cache:  cache,
	}

	// Pre-create file with non-zero content.
	filePath := filepath.Join(dir, "test.bin")
	content := []byte("XXXXXXXXXXXX") // 12 bytes
	if err := os.WriteFile(filePath, content, 0600); err != nil {
		t.Fatal(err)
	}

	files := make(map[string]*os.File)
	req := &apiv1.WriteRequest{
		Blocks: []*apiv1.ChangedBlock{
			{
				RequestId: 1,
				FilePath:  "test.bin",
				Offset:    4,
				Length:    4,
				TotalSize: 12,
				IsZero:    true,
			},
		},
	}

	if err := srv.writeBlocks(files, req); err != nil {
		t.Fatalf("writeBlocks failed: %v", err)
	}

	// Flush via SyncAndRelease before reading.
	if err := cache.SyncAndRelease("test.bin"); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal(err)
	}

	// Bytes 4-7 should be zero; rest should be 'X'.
	expected := []byte{
		'X', 'X', 'X', 'X',
		0, 0, 0, 0,
		'X', 'X', 'X', 'X',
	}
	for i, b := range got {
		if b != expected[i] {
			t.Errorf(
				"byte[%d] = %q, want %q",
				i, b, expected[i],
			)
		}
	}
}

// TestWriteBlocks_DataBlock verifies that non-zero blocks
// are written correctly.
func TestWriteBlocks_DataBlock(t *testing.T) {
	dir := t.TempDir()
	cache := NewWriteCache(dir)
	defer func() { _ = cache.Close() }()

	srv := &DataServer{
		logger: logr.Discard(),
		cache:  cache,
	}

	files := make(map[string]*os.File)
	payload := []byte("hello")
	req := &apiv1.WriteRequest{
		Blocks: []*apiv1.ChangedBlock{
			{
				RequestId: 2,
				FilePath:  "data.bin",
				Offset:    0,
				Length:    uint64(len(payload)),
				TotalSize: uint64(len(payload)),
				IsZero:    false,
				Data:      payload,
			},
		},
	}

	if err := srv.writeBlocks(files, req); err != nil {
		t.Fatalf("writeBlocks failed: %v", err)
	}

	if err := cache.SyncAndRelease("data.bin"); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "data.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Errorf("got %q, want %q", got, payload)
	}
}

// TestDataServer_Write_ZeroBlock exercises the full Write RPC
// path with a zero block.
func TestDataServer_Write_ZeroBlock(t *testing.T) {
	dir := t.TempDir()
	cache := NewWriteCache(dir)
	defer func() { _ = cache.Close() }()

	srv := &DataServer{
		logger: logr.Discard(),
		cache:  cache,
	}

	// Pre-create file with content.
	fpath := filepath.Join(dir, "rpc.bin")
	if err := os.WriteFile(fpath, []byte("ABCDEFGH"), 0600); err != nil {
		t.Fatal(err)
	}

	stream := &mockWriteBidiStream{
		req: &apiv1.WriteRequest{
			Blocks: []*apiv1.ChangedBlock{
				{
					RequestId: 10,
					FilePath:  "rpc.bin",
					Offset:    2,
					Length:    4,
					TotalSize: 8,
					IsZero:    true,
				},
			},
		},
	}

	if err := srv.Write(stream); err != nil {
		t.Fatalf("Write RPC failed: %v", err)
	}

	if stream.resp == nil {
		t.Fatal("no response received")
	}
	if len(stream.resp.AcknowledgedIds) != 1 ||
		stream.resp.AcknowledgedIds[0] != 10 {
		t.Errorf(
			"expected ack ID 10, got %v",
			stream.resp.AcknowledgedIds,
		)
	}

	got, err := os.ReadFile(fpath)
	if err != nil {
		t.Fatal(err)
	}

	expected := []byte{'A', 'B', 0, 0, 0, 0, 'G', 'H'}
	for i, b := range got {
		if b != expected[i] {
			t.Errorf(
				"byte[%d] = %q, want %q",
				i, b, expected[i],
			)
		}
	}
}
