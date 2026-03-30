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

package pipeline

import (
	"context"
	"testing"
)

type mockDataReader struct {
	data []byte
}

func (m *mockDataReader) ReadAt(
	_ string, offset, length int64,
) ([]byte, error) {
	end := offset + length
	if end > int64(len(m.data)) {
		end = int64(len(m.data))
	}
	return append([]byte(nil), m.data[offset:end]...), nil
}

func (m *mockDataReader) CloseFile(_ string) error {
	return nil
}

func TestStageRead_ReadsAllChunks(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.ReadWorkers = 2

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)
	win := NewWindowSemaphore(cfg.MaxWindow)

	data := make([]byte, 48)
	for i := range data {
		data[i] = 0xAB
	}
	reader := &mockDataReader{data: data}

	inCh := make(chan Chunk, 3)
	inCh <- Chunk{ReqID: 0, Offset: 0, Length: 16}
	inCh <- Chunk{ReqID: 1, Offset: 16, Length: 16}
	inCh <- Chunk{ReqID: 2, Offset: 32, Length: 16}
	close(inCh)

	readCh := make(chan ReadChunk, 3)

	err := StageRead(ctx, cfg, mem, win, reader, inCh, readCh)
	if err != nil {
		t.Fatal(err)
	}

	close(readCh)

	count := 0
	for range readCh {
		count++
	}
	if count != 3 {
		t.Fatalf("expected 3 chunks, got %d", count)
	}
}

func TestStageRead_ZeroShortCircuit(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.ReadWorkers = 1

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)
	win := NewWindowSemaphore(cfg.MaxWindow)

	data := make([]byte, 16) // all zeros
	reader := &mockDataReader{data: data}

	inCh := make(chan Chunk, 1)
	inCh <- Chunk{ReqID: 0, Offset: 0, Length: 16}
	close(inCh)

	readCh := make(chan ReadChunk, 1)

	err := StageRead(ctx, cfg, mem, win, reader, inCh, readCh)
	if err != nil {
		t.Fatal(err)
	}

	close(readCh)

	if len(readCh) != 1 {
		t.Fatal("expected 1 chunk on readCh")
	}
	rc := <-readCh
	if !rc.IsZero {
		t.Fatal("zero block should have IsZero=true")
	}
	if rc.Data != nil {
		t.Fatal("zero block should have nil Data")
	}
	if rc.Length != 16 {
		t.Fatalf("expected Length=16, got %d", rc.Length)
	}
}
