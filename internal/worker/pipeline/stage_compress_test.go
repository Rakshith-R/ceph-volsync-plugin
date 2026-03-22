package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"testing"

	"github.com/pierrec/lz4/v4"
)

func TestStageCompress_LZ4_Compresses(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.CompressWorkers = 1

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)

	data := bytes.Repeat([]byte{0xAA}, 4096)
	hash := sha256.Sum256(data)

	inCh := make(chan HashedChunk, 1)
	_ = mem.Acquire(ctx, cfg.ChunkSize)
	inCh <- HashedChunk{
		ReqID:  0,
		Offset: 0,
		Length: 4096,
		Data:   data,
		Hash:   hash,
		Held:   held{memRawN: cfg.ChunkSize, hasMem: true},
	}
	close(inCh)

	outCh := make(chan CompressedChunk, 1)

	err := StageCompress(ctx, cfg, mem, nil, inCh, outCh)
	if err != nil {
		t.Fatal(err)
	}
	close(outCh)

	cc := <-outCh
	if cc.UncompressedLength != 4096 {
		t.Fatalf("uncompressed len: got %d", cc.UncompressedLength)
	}
	if int64(len(cc.Data)) >= 4096 {
		t.Fatal("data should be compressed")
	}
	if cc.IsRaw {
		t.Fatal("should not be raw")
	}

	out := make([]byte, 4096)
	n, err := lz4.UncompressBlock(cc.Data, out)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out[:n], data) {
		t.Fatal("decompressed data mismatch")
	}
}

func TestStageCompress_LZ4_Incompressible(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.CompressWorkers = 1

	mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)

	data := make([]byte, 256)
	for i := range data {
		data[i] = byte(i)
	}
	hash := sha256.Sum256(data)

	inCh := make(chan HashedChunk, 1)
	_ = mem.Acquire(ctx, cfg.ChunkSize)
	inCh <- HashedChunk{
		ReqID:  0,
		Offset: 0,
		Length: 256,
		Data:   data,
		Hash:   hash,
		Held:   held{memRawN: cfg.ChunkSize, hasMem: true},
	}
	close(inCh)

	outCh := make(chan CompressedChunk, 1)

	err := StageCompress(ctx, cfg, mem, nil, inCh, outCh)
	if err != nil {
		t.Fatal(err)
	}
	close(outCh)

	cc := <-outCh
	if cc.UncompressedLength != 256 {
		t.Fatalf("uncompressed len: got %d", cc.UncompressedLength)
	}
	if !cc.IsRaw {
		t.Fatal("should be raw (incompressible)")
	}
}
