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

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"golang.org/x/sys/unix"
)

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

func TestWriteBlocks_ZeroBlockFallocate(t *testing.T) {
	f, err := os.CreateTemp("", "desttest-zero")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(f.Name()) }()
	_ = f.Truncate(1024)

	// Check if fallocate PUNCH_HOLE is supported on this filesystem
	if err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 0, 16); err != nil {
		t.Skipf("FALLOC_FL_PUNCH_HOLE not supported: %v", err)
	}

	// Write non-zero data first
	nonZero := make([]byte, 512)
	for i := range nonZero {
		nonZero[i] = 0xFF
	}
	if _, err := f.WriteAt(nonZero, 0); err != nil {
		t.Fatal(err)
	}

	srv := &RBDDataServer{logger: logr.Discard(), devicePath: f.Name()}
	err = srv.writeBlocks(f, &apiv1.WriteRequest{
		Blocks: []*apiv1.ChangedBlock{{
			Offset: 0, Length: 512, IsZero: true,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 512)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("byte %d not zero after fallocate: %x", i, b)
		}
	}
}
