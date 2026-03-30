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

func TestHeld_ReleaseAll(t *testing.T) {
	mem := NewMemSemaphore(1000)
	win := NewWindowSemaphore(64)
	ctx := context.Background()
	_ = mem.Acquire(ctx, 100)
	_ = win.Acquire(ctx, 0)
	h := held{reqID: 0, memRawN: 100, hasWin: true, hasMem: true}
	h.release(mem, win)
	if h.hasMem || h.hasWin {
		t.Fatal("flags not cleared")
	}
	if err := mem.Acquire(ctx, 100); err != nil {
		t.Fatal(err)
	}
	mem.Release(100)
}

func TestHeld_DoubleRelease(t *testing.T) {
	mem := NewMemSemaphore(1000)
	win := NewWindowSemaphore(64)
	ctx := context.Background()
	_ = mem.Acquire(ctx, 50)
	_ = win.Acquire(ctx, 0)
	h := held{reqID: 0, memRawN: 50, hasWin: true, hasMem: true}
	h.release(mem, win)
	h.release(mem, win) // should be safe no-op
}

func TestHeld_PartialReleaseMemRaw(t *testing.T) {
	mem := NewMemSemaphore(1000)
	ctx := context.Background()
	_ = mem.Acquire(ctx, 100)
	h := held{memRawN: 100, hasMem: true}
	h.partialReleaseMemRaw(mem, 30)
	if h.memRawN != 70 {
		t.Fatalf("expected 70, got %d", h.memRawN)
	}
}

func TestHeld_ReleaseMemOnly(t *testing.T) {
	mem := NewMemSemaphore(1000)
	win := NewWindowSemaphore(64)
	ctx := context.Background()
	_ = mem.Acquire(ctx, 100)
	_ = win.Acquire(ctx, 0)
	h := held{
		reqID: 0, memRawN: 100,
		hasWin: true, hasMem: true,
	}
	h.releaseMemOnly(mem)
	if h.hasMem {
		t.Fatal("hasMem should be cleared")
	}
	if !h.hasWin {
		t.Fatal("hasWin should still be set")
	}
	// Memory should be reclaimable now.
	if err := mem.Acquire(ctx, 100); err != nil {
		t.Fatal(err)
	}
	mem.Release(100)
	// Clean up window.
	win.Release(0)
}
