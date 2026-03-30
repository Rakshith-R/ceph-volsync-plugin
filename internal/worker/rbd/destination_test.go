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
	"os"
	"testing"

	"github.com/go-logr/logr"
	"github.com/pierrec/lz4/v4"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func TestWriteBlocks_LZ4Compressed(t *testing.T) {
	f, err := os.CreateTemp("", "desttest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	_ = f.Truncate(1024)

	original := []byte("hello world data")
	maxDst := lz4.CompressBlockBound(len(original))
	compressed := make([]byte, maxDst)
	n, err := lz4.CompressBlock(original, compressed, nil)
	if err != nil || n == 0 {
		t.Fatal("compression failed")
	}
	compressed = compressed[:n]

	srv := &RBDDataServer{logger: logr.Discard(), devicePath: f.Name()}
	err = srv.writeBlocks(f, &apiv1.WriteRequest{
		Blocks: []*apiv1.ChangedBlock{{
			Offset: 0, Length: uint64(len(original)),
			Data: compressed, Compression: apiv1.CompressionAlgo_COMPRESSION_LZ4,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(original))
	_, _ = f.ReadAt(buf, 0)
	if string(buf) != string(original) {
		t.Fatalf("data mismatch: %q", buf)
	}
}

func TestWriteBlocks_Uncompressed(t *testing.T) {
	f, err := os.CreateTemp("", "desttest")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	_ = f.Truncate(1024)

	data := []byte("raw block data!!")
	srv := &RBDDataServer{logger: logr.Discard(), devicePath: f.Name()}
	err = srv.writeBlocks(f, &apiv1.WriteRequest{
		Blocks: []*apiv1.ChangedBlock{{
			Offset: 0, Length: uint64(len(data)), Data: data,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len(data))
	_, _ = f.ReadAt(buf, 0)
	if string(buf) != string(data) {
		t.Fatalf("data mismatch: %q", buf)
	}
}
