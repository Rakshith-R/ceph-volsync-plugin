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

// partialReleaseMemRaw returns delta bytes from the raw memory pool (LZ4 in-place shrink).
func (h *held) partialReleaseMemRaw(memRaw *MemSemaphore, delta int64) {
	memRaw.PartialRelease(delta)
	h.memRawN -= delta
}
