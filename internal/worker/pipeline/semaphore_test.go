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
	"sync"
	"testing"
	"time"
)

func TestMemSemaphore_AcquireRelease(t *testing.T) {
	s := NewMemSemaphore(100)
	ctx := context.Background()
	if err := s.Acquire(ctx, 100); err != nil {
		t.Fatal(err)
	}
	s.Release(100)
	if err := s.Acquire(ctx, 100); err != nil {
		t.Fatal(err)
	}
	s.Release(100)
}

func TestMemSemaphore_BlocksWhenFull(t *testing.T) {
	s := NewMemSemaphore(10)
	ctx := context.Background()
	_ = s.Acquire(ctx, 10)

	done := make(chan struct{})
	go func() {
		_ = s.Acquire(ctx, 5)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("should block")
	case <-time.After(50 * time.Millisecond):
	}

	s.Release(5)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("should unblock")
	}
	s.Release(10)
}

func TestMemSemaphore_FIFO(t *testing.T) {
	s := NewMemSemaphore(10)
	ctx := context.Background()
	_ = s.Acquire(ctx, 10)

	var mu sync.Mutex
	var order []int

	for i := range 3 {
		go func() {
			_ = s.Acquire(ctx, 5)
			mu.Lock()
			order = append(order, i)
			mu.Unlock()
		}()
		time.Sleep(10 * time.Millisecond)
	}

	for range 3 {
		s.Release(5)
		time.Sleep(20 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	for i := range len(order) - 1 {
		if order[i] > order[i+1] {
			t.Fatalf("not FIFO: %v", order)
		}
	}
}

func TestMemSemaphore_ContextCancel(t *testing.T) {
	s := NewMemSemaphore(10)
	ctx := context.Background()
	_ = s.Acquire(ctx, 10)

	ctx2, cancel := context.WithCancel(ctx)
	cancel()

	err := s.Acquire(ctx2, 5)
	if err != context.Canceled {
		t.Fatalf("expected Canceled, got %v", err)
	}
	s.Release(10)
}

func TestMemSemaphore_PartialRelease(t *testing.T) {
	s := NewMemSemaphore(100)
	ctx := context.Background()
	_ = s.Acquire(ctx, 80)
	s.PartialRelease(30)
	if err := s.Acquire(ctx, 50); err != nil {
		t.Fatal(err)
	}
	s.Release(50)
	s.Release(50)
}

func TestWindowSemaphore_Sequential(t *testing.T) {
	w := NewWindowSemaphore(4)
	ctx := context.Background()
	for i := range uint64(4) {
		if err := w.Acquire(ctx, i); err != nil {
			t.Fatal(err)
		}
	}
	w.Release(0)
	w.Release(1)
	if err := w.Acquire(ctx, 6); err != nil {
		t.Fatal(err)
	}
	if err := w.Acquire(ctx, 7); err != nil {
		t.Fatal(err)
	}
	w.Release(2)
	w.Release(3)
	w.Release(6)
	w.Release(7)
}

func TestWindowSemaphore_BlocksAtLimit(t *testing.T) {
	w := NewWindowSemaphore(4)
	ctx := context.Background()
	for i := range uint64(8) {
		if err := w.Acquire(ctx, i); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan struct{})
	go func() {
		_ = w.Acquire(ctx, 8)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("should block")
	case <-time.After(50 * time.Millisecond):
	}
	w.Release(0)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("should unblock")
	}
	for i := uint64(1); i <= 8; i++ {
		w.Release(i)
	}
}

func TestWindowSemaphore_BaseAdvancesMultiple(t *testing.T) {
	w := NewWindowSemaphore(8)
	ctx := context.Background()
	for i := range uint64(4) {
		_ = w.Acquire(ctx, i)
	}
	w.Release(1)
	w.Release(0)
	w.Release(2)
	w.Release(3)
	// base should now be at 4
	if err := w.Acquire(ctx, 10); err != nil {
		t.Fatal(err)
	}
	w.Release(10)
}

func TestWindowSemaphore_ContextCancel(t *testing.T) {
	w := NewWindowSemaphore(2)
	ctx := context.Background()
	for i := range uint64(4) {
		_ = w.Acquire(ctx, i)
	}
	ctx2, cancel := context.WithCancel(ctx)
	cancel()
	err := w.Acquire(ctx2, 4)
	if err != context.Canceled {
		t.Fatalf("expected Canceled, got %v", err)
	}
	for i := range uint64(4) {
		w.Release(i)
	}
}

func TestWindowSemaphore_IsReleased(t *testing.T) {
	w := NewWindowSemaphore(4)
	ctx := context.Background()
	for i := range uint64(4) {
		_ = w.Acquire(ctx, i)
	}

	// Before release, IsReleased should be false.
	if w.IsReleased(0) {
		t.Fatal("reqID 0 should not be released yet")
	}

	// Release 0 — base advances to 1.
	w.Release(0)
	if !w.IsReleased(0) {
		t.Fatal("reqID 0 should be released after Release(0)")
	}
	if w.IsReleased(1) {
		t.Fatal("reqID 1 should not be released yet")
	}

	// Release out of order: 2, then 1 — base
	// should advance to 3.
	w.Release(2)
	if w.IsReleased(2) {
		t.Fatal("reqID 2 released but base blocked by 1")
	}
	w.Release(1)
	if !w.IsReleased(2) {
		t.Fatal("reqID 2 should be released after 1 unblocks base")
	}

	w.Release(3)
}

func TestWindowSemaphore_PressureSignal(t *testing.T) {
	w := NewWindowSemaphore(4)
	ctx := context.Background()
	sig := w.PressureSignal(0.75)
	for i := range uint64(3) {
		_ = w.Acquire(ctx, i)
	}
	select {
	case <-sig:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("pressure should fire at 75%")
	}
	w.Release(0)
	sig2 := w.PressureSignal(0.75)
	select {
	case <-sig2:
		t.Fatal("should not fire at 50%")
	case <-time.After(50 * time.Millisecond):
	}
	w.Release(1)
	w.Release(2)
}
