package pipeline

import (
	"context"
	"crypto/sha256"
	"testing"
)

func TestStageHash_CorrectHash(t *testing.T) {
	ctx := context.Background()
	cfg := &Config{}
	cfg.SetDefaults()
	cfg.HashWorkers = 1

	data := []byte("hello world block data")
	expected := sha256.Sum256(data)

	inCh := make(chan ReadChunk, 1)
	inCh <- ReadChunk{ReqID: 0, Offset: 0, Data: data}
	close(inCh)

	outCh := make(chan HashedChunk, 1)

	err := StageHash(ctx, cfg, inCh, outCh)
	if err != nil {
		t.Fatal(err)
	}
	close(outCh)

	hc := <-outCh
	if hc.Hash != expected {
		t.Fatal("hash mismatch")
	}
	if hc.ReqID != 0 {
		t.Fatal("reqID mismatch")
	}
	if hc.Length != int64(len(data)) {
		t.Fatalf("length: expected %d, got %d", len(data), hc.Length)
	}
}
