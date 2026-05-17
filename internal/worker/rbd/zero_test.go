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
)

// zeroRange on a regular file falls back to WriteZeros
// since BLKZEROOUT only works on block devices.

func TestZeroRange_FallbackOnRegularFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "zero-rbd-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	data := []byte("ABCDEFGHIJKLMNOP")
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	// BLKZEROOUT will fail on regular file, fallback writes zeros.
	if err := zeroRange(f, 4, 8); err != nil {
		t.Fatalf("zeroRange failed: %v", err)
	}

	buf := make([]byte, 16)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}

	expected := []byte{'A', 'B', 'C', 'D', 0, 0, 0, 0, 0, 0, 0, 0, 'M', 'N', 'O', 'P'}
	for i, b := range buf {
		if b != expected[i] {
			t.Errorf("byte[%d] = %d, want %d", i, b, expected[i])
		}
	}
}

func TestZeroRange_EntireRange(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "zero-rbd-full-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	data := make([]byte, 4096)
	for i := range data {
		data[i] = 0xFF
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	if err := zeroRange(f, 0, 4096); err != nil {
		t.Fatalf("zeroRange failed: %v", err)
	}

	buf := make([]byte, 4096)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Errorf("byte[%d] = %d, want 0", i, b)
			break
		}
	}
}

func TestZeroRange_LargerThanFallbackBuffer(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "zero-rbd-large-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	// 2MB — tests chunked fallback path.
	size := int64(2 * 1024 * 1024)
	data := make([]byte, size)
	for i := range data {
		data[i] = 0xDD
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	if err := zeroRange(f, 0, size); err != nil {
		t.Fatalf("zeroRange failed: %v", err)
	}

	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Errorf("byte[%d] = %d, want 0", i, b)
			break
		}
	}
}

func TestZeroRange_AtOffset(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "zero-rbd-off-*")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	data := make([]byte, 8192)
	for i := range data {
		data[i] = 0xBB
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal(err)
	}

	if err := zeroRange(f, 4096, 4096); err != nil {
		t.Fatalf("zeroRange failed: %v", err)
	}

	buf := make([]byte, 8192)
	if _, err := f.ReadAt(buf, 0); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 4096; i++ {
		if buf[i] != 0xBB {
			t.Errorf("byte[%d] = %d, want 0xBB", i, buf[i])
			break
		}
	}
	for i := 4096; i < 8192; i++ {
		if buf[i] != 0 {
			t.Errorf("byte[%d] = %d, want 0", i, buf[i])
			break
		}
	}
}
