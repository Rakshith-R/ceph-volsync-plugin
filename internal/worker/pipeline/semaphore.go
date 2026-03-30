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
	"container/list"
	"context"
	"sync"
)

type memWaiter struct {
	n  int64
	ch chan struct{}
}

// MemSemaphore is a weighted semaphore with channel-per-waiter FIFO wake discipline.
type MemSemaphore struct {
	mu       sync.Mutex
	capacity int64
	cur      int64
	waiters  list.List
}

func NewMemSemaphore(capacity int64) *MemSemaphore {
	return &MemSemaphore{capacity: capacity}
}

func (s *MemSemaphore) Acquire(ctx context.Context, n int64) error {
	s.mu.Lock()
	if s.cur+n <= s.capacity && s.waiters.Len() == 0 {
		s.cur += n
		s.mu.Unlock()
		return nil
	}

	w := memWaiter{n: n, ch: make(chan struct{})}
	elem := s.waiters.PushBack(&w)
	s.mu.Unlock()

	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		select {
		case <-w.ch:
			s.mu.Unlock()
			return nil
		default:
			s.waiters.Remove(elem)
			s.mu.Unlock()
			return ctx.Err()
		}
	}
}

func (s *MemSemaphore) Release(n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cur -= n
	s.wakeWaiters()
}

// PartialRelease returns n bytes (used by LZ4 in-place shrink).
func (s *MemSemaphore) PartialRelease(n int64) {
	s.Release(n)
}

func (s *MemSemaphore) wakeWaiters() {
	for {
		front := s.waiters.Front()
		if front == nil {
			break
		}
		w := front.Value.(*memWaiter)
		if s.cur+w.n > s.capacity {
			break
		}
		s.cur += w.n
		s.waiters.Remove(front)
		close(w.ch)
	}
}

type winWaiter struct {
	reqID uint64
	ch    chan struct{}
}

// WindowSemaphore bounds the spread of in-flight request IDs.
type WindowSemaphore struct {
	mu        sync.Mutex
	maxWindow int
	limit     int
	base      uint64
	released  []bool
	inFlight  int
	waiters   []*winWaiter // sorted by reqID
	pressure  []pressureEntry
}

type pressureEntry struct {
	threshold float64
	ch        chan struct{}
	fired     bool
}

func NewWindowSemaphore(maxWindow int) *WindowSemaphore {
	limit := 2 * maxWindow
	return &WindowSemaphore{
		maxWindow: maxWindow,
		limit:     limit,
		released:  make([]bool, limit),
	}
}

func (w *WindowSemaphore) Acquire(ctx context.Context, reqID uint64) error {
	w.mu.Lock()
	if reqID-w.base < uint64(w.limit) { //nolint:gosec // G115: limit is positive
		w.inFlight++
		w.checkPressure()
		w.mu.Unlock()
		return nil
	}

	waiter := &winWaiter{reqID: reqID, ch: make(chan struct{})}
	w.insertWaiter(waiter)
	w.mu.Unlock()

	select {
	case <-waiter.ch:
		return nil
	case <-ctx.Done():
		w.mu.Lock()
		select {
		case <-waiter.ch:
			w.mu.Unlock()
			return nil
		default:
			w.removeWaiter(waiter)
			w.mu.Unlock()
			return ctx.Err()
		}
	}
}

func (w *WindowSemaphore) Release(reqID uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Use absolute (ring-buffer) indexing, consistent
	// with the advance loop below.
	idx := int(reqID % uint64(w.limit)) //nolint:gosec // G115: result in [0, limit)
	w.released[idx] = true
	w.inFlight--

	for w.released[int(w.base%uint64(w.limit))] { //nolint:gosec // G115: bounded by modulo
		w.released[int(w.base%uint64(w.limit))] = false //nolint:gosec // G115: bounded by modulo
		w.base++
	}

	w.wakeWinWaiters()
	w.checkPressure()
}

// IsReleased returns true if reqID has been released
// and its window slot has been reclaimed (base > reqID).
func (w *WindowSemaphore) IsReleased(reqID uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.base > reqID
}

func (w *WindowSemaphore) PressureSignal(threshold float64) <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	ch := make(chan struct{}, 1)
	entry := pressureEntry{threshold: threshold, ch: ch}

	if float64(w.inFlight) >= threshold*float64(w.maxWindow) {
		ch <- struct{}{}
		entry.fired = true
	}

	w.pressure = append(w.pressure, entry)
	return ch
}

func (w *WindowSemaphore) insertWaiter(waiter *winWaiter) {
	i := 0
	for i < len(w.waiters) && w.waiters[i].reqID < waiter.reqID {
		i++
	}
	w.waiters = append(w.waiters, nil)
	copy(w.waiters[i+1:], w.waiters[i:])
	w.waiters[i] = waiter
}

func (w *WindowSemaphore) removeWaiter(waiter *winWaiter) {
	for i, wt := range w.waiters {
		if wt == waiter {
			w.waiters = append(w.waiters[:i], w.waiters[i+1:]...)
			return
		}
	}
}

func (w *WindowSemaphore) wakeWinWaiters() {
	for len(w.waiters) > 0 {
		front := w.waiters[0]
		if front.reqID-w.base >= uint64(w.limit) { //nolint:gosec // G115: limit is positive
			break
		}
		w.waiters = w.waiters[1:]
		w.inFlight++
		close(front.ch)
	}
}

func (w *WindowSemaphore) checkPressure() {
	for i := range w.pressure {
		p := &w.pressure[i]
		shouldFire := float64(w.inFlight) >= p.threshold*float64(w.maxWindow)
		if shouldFire && !p.fired {
			select {
			case p.ch <- struct{}{}:
			default:
			}
			p.fired = true
		} else if !shouldFire {
			p.fired = false
		}
	}
}
