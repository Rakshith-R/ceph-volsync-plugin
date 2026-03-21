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
	cfg.setDefaults()
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
	zeroCh := make(chan ZeroChunk, 3)

	err := StageRead(ctx, cfg, mem, win, reader, inCh, readCh, zeroCh)
	if err != nil {
		t.Fatal(err)
	}

	close(readCh)
	close(zeroCh)

	count := 0
	for range readCh {
		count++
	}
	for range zeroCh {
		count++
	}
	if count != 3 {
		t.Fatalf("expected 3 chunks, got %d", count)
	}
}

func TestStageRead_ZeroShortCircuit(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.setDefaults()
	cfg.ReadWorkers = 1

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)
	win := NewWindowSemaphore(cfg.MaxWindow)

	data := make([]byte, 16) // all zeros
	reader := &mockDataReader{data: data}

	inCh := make(chan Chunk, 1)
	inCh <- Chunk{ReqID: 0, Offset: 0, Length: 16}
	close(inCh)

	readCh := make(chan ReadChunk, 1)
	zeroCh := make(chan ZeroChunk, 1)

	err := StageRead(ctx, cfg, mem, win, reader, inCh, readCh, zeroCh)
	if err != nil {
		t.Fatal(err)
	}

	close(readCh)
	close(zeroCh)

	if len(readCh) != 0 {
		t.Fatal("zero block should not go to readCh")
	}
	if len(zeroCh) != 1 {
		t.Fatal("zero block should go to zeroCh")
	}
}
