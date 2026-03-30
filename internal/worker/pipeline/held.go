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

// held tracks semaphore resources owned by a chunk as it moves through the pipeline.
// All exit paths must call release().
type held struct {
	reqID   uint64
	memRawN int64
	hasWin  bool
	hasMem  bool
}

// release frees all held resources in reverse acquisition order: raw -> win.
func (h *held) release(memRaw *MemSemaphore, win *WindowSemaphore) {
	if h.hasMem {
		memRaw.Release(h.memRawN)
		h.hasMem = false
	}
	if h.hasWin {
		win.Release(h.reqID)
		h.hasWin = false
	}
}

// releaseMemOnly frees only the raw memory pool
// (not the window). Used by StageSendData after
// stream.Send(); window is released by ack receiver.
func (h *held) releaseMemOnly(memRaw *MemSemaphore) {
	if h.hasMem {
		memRaw.Release(h.memRawN)
		h.hasMem = false
	}
}

// partialReleaseMemRaw returns delta bytes from the raw memory pool (LZ4 in-place shrink).
func (h *held) partialReleaseMemRaw(memRaw *MemSemaphore, delta int64) {
	memRaw.PartialRelease(delta)
	h.memRawN -= delta
}
