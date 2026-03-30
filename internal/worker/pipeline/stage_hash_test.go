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

	memRaw := NewMemSemaphore(256 * 1024 * 1024)
	win := NewWindowSemaphore(64)

	err := StageHash(ctx, cfg, memRaw, win, inCh, outCh)
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
