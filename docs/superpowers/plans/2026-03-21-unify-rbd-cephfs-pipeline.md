# Unify RBD and CephFS Block-Streaming Pipeline

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended)
> or superpowers:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax
> for tracking.

**Goal:** Move `internal/worker/rbd/pipeline/` to
`internal/worker/pipeline/`, introduce a `DataReader`
interface so CephFS can use the same 5-stage pipeline
(hash-dedup + LZ4 + delayed commit) as RBD.

**Architecture:** A new `DataReader` interface replaces
`io.ReaderAt` in `StageRead` — flat-device reads (RBD)
and per-file filesystem reads (CephFS) implement it.
`StageSendData` gains delayed-commit logic:
CommitRequest for file N sent only after req_id
`N + MaxWindow + 2` has been processed.
`pipeline.Run()` accepts a nil `hashClient` (CephFS
initially may skip dedup if desired, though the plan
wires it in).

**Tech Stack:** Go 1.22+, go-ceph (CGO), gRPC,
`golang.org/x/sync/errgroup`, LZ4, SHA-256.

---

## File Map

| Path | Action | Role |
|------|---------|------|
| `internal/worker/pipeline/` | Move (was `rbd/pipeline/`) | Shared pipeline package |
| `internal/worker/pipeline/reader.go` | Create | `DataReader` interface |
| `internal/worker/pipeline/stage_read.go` | Modify | Use `DataReader` instead of `io.ReaderAt` |
| `internal/worker/pipeline/semaphore.go` | Modify | Add `IsReleased` |
| `internal/worker/pipeline/semaphore_test.go` | Modify | Test `IsReleased` |
| `internal/worker/pipeline/stage_send_data.go` | Modify | Delayed commit + `CloseFile` |
| `internal/worker/pipeline/stage_send_data_test.go` | Modify | Test file-boundary commit |
| `internal/worker/pipeline/pipeline.go` | Modify | Pass `DataReader` + nil hashClient |
| `internal/worker/pipeline/pipeline_test.go` | Modify | Mock `DataReader` |
| `internal/worker/pipeline/stage_read_test.go` | Modify | Mock `DataReader` |
| `internal/worker/rbd/device_reader.go` | Create | `DeviceReader`: flat block device |
| `internal/worker/rbd/source.go` | Modify | Import path + use `DeviceReader` |
| `internal/worker/cephfs/block_iterator.go` | Create | `CephFSBlockIterator` (no CGO) |
| `internal/worker/cephfs/block_iterator_test.go` | Create | Unit tests |
| `internal/worker/cephfs/cephfs_reader.go` | Create | `CephFSReader` (no CGO) |
| `internal/worker/cephfs/cephfs_reader_test.go` | Create | Unit tests |
| `internal/worker/cephfs/hash_server.go` | Create | `CephFSHashServer` (no CGO) |
| `internal/worker/cephfs/hash_server_test.go` | Create | Unit tests |
| `internal/worker/cephfs/destination.go` | Modify | Use `RunWithHash` |
| `internal/worker/cephfs/source.go` | Modify | Replace `streamBlockDiff` |
| `internal/worker/common/streamer.go` | Delete | Replaced by pipeline |

---

## Task 1: Move pipeline package

**Files:**
- Move: `internal/worker/rbd/pipeline/` →
  `internal/worker/pipeline/`
- Modify: `internal/worker/rbd/source.go`

- [ ] **Step 1: Move the directory**

```bash
cd /home/rakshithr4/Workspace/github.com/RamenDR/worktree/mover-rbd
git mv internal/worker/rbd/pipeline \
       internal/worker/pipeline
```

- [ ] **Step 2: Update the package declaration**

All moved files declare `package pipeline` — no
change needed. Update the import path in
`internal/worker/rbd/source.go` (line 32):

```go
// Before
"github.com/RamenDR/ceph-volsync-plugin/internal/worker/rbd/pipeline"

// After
"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
```

The `pipeline.ChangeBlock` reference on line 428
(`return &pipeline.ChangeBlock{...}`) needs no change
— the type name stays the same; only the import path
changes.

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git commit -sm"refactor(pipeline): move pipeline package to internal/worker/pipeline"
```

---

## Task 2: DataReader interface

**Files:**
- Create: `internal/worker/pipeline/reader.go`
- Modify: `internal/worker/pipeline/stage_read.go`
- Modify: `internal/worker/pipeline/pipeline.go`
- Modify: `internal/worker/pipeline/pipeline_test.go`
- Modify: `internal/worker/pipeline/stage_read_test.go`

- [ ] **Step 1: Write the failing test**

Add to `stage_read_test.go` (or in a new helper):

```go
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
    return m.data[offset:end], nil
}

func (m *mockDataReader) CloseFile(_ string) error {
    return nil
}
```

Run:
```bash
make test
```

Expected: compile error — `DataReader` type not found.

- [ ] **Step 2: Create `reader.go`**

```go
package pipeline

// DataReader abstracts block-level reads.
// Implementations must be safe for concurrent calls
// from multiple goroutines (ReadWorkers > 1).
type DataReader interface {
    // ReadAt reads length bytes from filePath at
    // offset. filePath is ignored for flat devices.
    ReadAt(
        filePath string, offset, length int64,
    ) ([]byte, error)
    // CloseFile releases resources held for filePath
    // after its CommitRequest has been sent.
    // No-op for flat block-device readers.
    CloseFile(filePath string) error
}
```

- [ ] **Step 3: Update `stage_read.go`**

Replace the `device io.ReaderAt` parameter with
`reader DataReader` in both `StageRead` and
`readWorker` signatures. Replace the read logic:

```go
// Before (lines 67-74 of stage_read.go)
data := make([]byte, chunk.Length)
n, err := device.ReadAt(data, chunk.Offset)
if err != nil && err != io.EOF {
    memRaw.Release(cfg.ChunkSize)
    win.Release(chunk.ReqID)
    return fmt.Errorf("pread at offset %d: %w",
        chunk.Offset, err)
}
data = data[:n]

// After
data, err := reader.ReadAt(
    chunk.FilePath, chunk.Offset, chunk.Length,
)
if err != nil {
    memRaw.Release(cfg.ChunkSize)
    win.Release(chunk.ReqID)
    return fmt.Errorf("read chunk %d: %w",
        chunk.ReqID, err)
}
```

Remove `"io"` import from `stage_read.go` (no longer
uses `io.EOF` or `io.ReaderAt`).

- [ ] **Step 4: Update `pipeline.go` signature**

```go
// Before
func (p *Pipeline) Run(
    ctx context.Context,
    iter BlockIterator,
    device io.ReaderAt,
    stream grpc.ClientStreamingClient[...],
    hashClient apiv1.HashServiceClient,
) error

// After
func (p *Pipeline) Run(
    ctx context.Context,
    iter BlockIterator,
    reader DataReader,
    stream grpc.ClientStreamingClient[...],
    hashClient apiv1.HashServiceClient,
) error
```

Update the `StageRead` call inside `Run()`:
```go
// Before
return StageRead(gctx, cfg, memRaw, win, device, chunkCh, readCh, zeroCh)
// After
return StageRead(gctx, cfg, memRaw, win, reader, chunkCh, readCh, zeroCh)
```

Remove `"io"` from `pipeline.go` imports if it was
only used for `io.ReaderAt`.

- [ ] **Step 5: Update tests in `pipeline_test.go`**

The `TestPipeline_*` tests pass `bytes.NewReader(data)`
as the device. Replace with `&mockDataReader{data: data}`.
Add `mockDataReader` to a `_test.go` helper file
(or inline in `pipeline_test.go`).

- [ ] **Step 6: Run tests**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git commit -sm"feat(pipeline): replace io.ReaderAt with DataReader interface"
```

---

## Task 3: RBD DeviceReader adapter

**Files:**
- Create: `internal/worker/rbd/device_reader.go`
- Modify: `internal/worker/rbd/source.go`

- [ ] **Step 1: Create `device_reader.go`**

No build tag — pure Go, no CGO.

```go
package rbd

import (
    "fmt"
    "io"

    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

// DeviceReader wraps io.ReaderAt to satisfy
// pipeline.DataReader. The filePath argument is
// ignored because RBD uses a single flat device.
type DeviceReader struct {
    device io.ReaderAt
}

// ReadAt reads length bytes from the flat device at
// offset. filePath is ignored.
func (d *DeviceReader) ReadAt(
    _ string, offset, length int64,
) ([]byte, error) {
    data := make([]byte, length)
    n, err := d.device.ReadAt(data, offset)
    if err != nil && err != io.EOF {
        return nil, fmt.Errorf(
            "pread at offset %d: %w", offset, err,
        )
    }
    return data[:n], nil
}

// CloseFile is a no-op for flat block devices.
func (d *DeviceReader) CloseFile(_ string) error {
    return nil
}

var _ pipeline.DataReader = (*DeviceReader)(nil)
```

- [ ] **Step 2: Update `rbd/source.go`**

In the `Sync()` method (around line 127), change:

```go
// Before
device, err := os.Open(common.DevicePath)
if err != nil {
    return fmt.Errorf(
        "failed to open %s: %w",
        common.DevicePath, err,
    )
}
defer func() { _ = device.Close() }()

// ...
if err := p.Run(ctx, &rbdIterAdapter{iter: iter}, device, stream, hashClient); err != nil {

// After
devFile, err := os.Open(common.DevicePath)
if err != nil {
    return fmt.Errorf(
        "failed to open %s: %w",
        common.DevicePath, err,
    )
}
defer func() { _ = devFile.Close() }()
dr := &DeviceReader{device: devFile}

// ...
if err := p.Run(ctx, &rbdIterAdapter{iter: iter}, dr, stream, hashClient); err != nil {
```

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git commit -sm"feat(rbd): add DeviceReader adapter for pipeline DataReader interface"
```

---

## Task 4: WindowSemaphore.IsReleased

**Files:**
- Modify: `internal/worker/pipeline/semaphore.go`
- Modify: `internal/worker/pipeline/semaphore_test.go`

- [ ] **Step 1: Write the failing test**

Add to `semaphore_test.go`:

```go
func TestWindowSemaphore_IsReleased(t *testing.T) {
    ctx := context.Background()
    w := NewWindowSemaphore(8)

    _ = w.Acquire(ctx, 0)
    _ = w.Acquire(ctx, 1)

    if w.IsReleased(0) {
        t.Fatal("reqID 0 should not be released yet")
    }

    w.Release(0)
    if !w.IsReleased(0) {
        t.Fatal("reqID 0 should be released after Release(0)")
    }

    // reqID 1 still in-flight
    if w.IsReleased(1) {
        t.Fatal("reqID 1 should not be released yet")
    }
}
```

Run:
```bash
make test
```

Expected: FAIL — `IsReleased` undefined.

- [ ] **Step 2: Add `IsReleased` to `semaphore.go`**

After the `Release` method, add:

```go
// IsReleased returns true when all req_ids up to
// and including reqID have been released from the
// window (i.e., base > reqID).
func (w *WindowSemaphore) IsReleased(
    reqID uint64,
) bool {
    w.mu.Lock()
    defer w.mu.Unlock()
    return w.base > reqID
}
```

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git commit -sm"feat(pipeline): add WindowSemaphore.IsReleased for delayed commit"
```

---

## Task 5: StageSendData — delayed commit + CloseFile

**Files:**
- Modify: `internal/worker/pipeline/stage_send_data.go`
- Modify: `internal/worker/pipeline/stage_send_data_test.go`
- Modify: `internal/worker/pipeline/pipeline.go`
  (pass reader to StageSendData)

### Background

`StageSendData` currently only sends `WriteRequest`
messages — no `CommitRequest`. Under the new design it
sends a `CommitRequest` when a file boundary is
detected, but delays it until
`currentReqID >= lastReqID + MaxWindow + 2`.
After sending a commit it calls `reader.CloseFile`.
This works for both RBD (single file, commit at end)
and CephFS (one commit per file).

The `sendCommit` helper already exists in
`internal/worker/common/streamer.go` — copy it into
the pipeline package (it will be deleted from common
later). It sends a `SyncRequest_Commit` message.

- [ ] **Step 1: Write the failing test**

Add to `stage_send_data_test.go`:

```go
func TestStageSendData_CommitOnFileBoundary(t *testing.T) {
    ctx := context.Background()
    cfg := &Config{}
    cfg.setDefaults()

    mem := NewMemSemaphore(cfg.MaxRawMemoryBytes)
    win := NewWindowSemaphore(cfg.MaxWindow)

    inCh := make(chan CompressedChunk, 10)
    stream := &mockSyncStream{}
    reader := &mockDataReader{data: []byte{}}

    // Two chunks for "file-a", then two for "file-b".
    // Acquire mem+win here; StageSendData releases
    // them via held.release() after each flush —
    // do NOT call win.Release manually (double-release
    // corrupts WindowSemaphore.base).
    for i := range uint64(4) {
        path := "file-a"
        if i >= 2 {
            path = "file-b"
        }
        _ = mem.Acquire(ctx, 16)
        _ = win.Acquire(ctx, i)
        inCh <- CompressedChunk{
            ReqID:    i,
            FilePath: path,
            Offset:   int64(i) * 100,
            Data:     []byte("data"),
            UncompressedLength: 4,
            Held: held{
                reqID: i, memRawN: 16,
                hasWin: true, hasMem: true,
            },
        }
    }
    close(inCh)

    err := StageSendData(
        ctx, cfg, mem, win, stream, inCh, reader,
    )
    if err != nil {
        t.Fatal(err)
    }

    var commits int
    for _, req := range stream.sent {
        if _, ok := req.Operation.(*apiv1.SyncRequest_Commit); ok {
            commits++
        }
    }
    // Expect commit for file-a and file-b
    if commits < 1 {
        t.Fatalf("expected at least 1 commit, got %d", commits)
    }
}
```

Run:
```bash
make test
```

Expected: compile error — `StageSendData` wrong
signature.

- [ ] **Step 2: Add `sendCommit` helper to pipeline**

At the bottom of `stage_send_data.go`, add the
`sendCommit` function copied from
`internal/worker/common/streamer.go`:

```go
// sendCommit sends a CommitRequest on stream.
func sendCommit(
    stream grpc.ClientStreamingClient[
        apiv1.SyncRequest, apiv1.SyncResponse,
    ],
    path string,
) error {
    err := stream.Send(&apiv1.SyncRequest{
        Operation: &apiv1.SyncRequest_Commit{
            Commit: &apiv1.CommitRequest{
                Path: path,
            },
        },
    })
    if err != nil {
        if err == io.EOF {
            if _, recvErr := stream.CloseAndRecv();
                recvErr != nil {
                return fmt.Errorf(
                    "destination error during "+
                        "commit for %s: %w",
                    path, recvErr,
                )
            }
            return nil
        }
        return fmt.Errorf(
            "failed to send commit for %s: %w",
            path, err,
        )
    }
    return nil
}
```

Add `"fmt"`, `"io"` to imports.

- [ ] **Step 3: Rewrite `dataSendWorker`**

Update the signature to accept `reader DataReader`
and add delayed commit logic:

```go
type pendingCommit struct {
    path      string
    lastReqID uint64
}

func StageSendData(
    ctx context.Context,
    cfg *Config,
    memRaw *MemSemaphore,
    win *WindowSemaphore,
    stream grpc.ClientStreamingClient[
        apiv1.SyncRequest, apiv1.SyncResponse,
    ],
    inCh <-chan CompressedChunk,
    reader DataReader,
) error {
    return dataSendWorker(
        ctx, cfg, memRaw, win, stream, inCh, reader,
    )
}
```

Inside `dataSendWorker`, add a `pending []pendingCommit`
slice. When `curPath` changes (file boundary):

```go
// Drain commits ready to send:
// currentReqID >= head.lastReqID + MaxWindow + 2
drainPending := func(currentReqID uint64) error {
    for len(pending) > 0 {
        head := pending[0]
        if currentReqID < head.lastReqID+
            uint64(cfg.MaxWindow)+2 { //nolint:gosec
            break
        }
        if err := sendCommit(stream, head.path);
            err != nil {
            return err
        }
        if err := reader.CloseFile(head.path);
            err != nil {
            return err
        }
        pending = pending[1:]
    }
    return nil
}

// File boundary: enqueue commit for previous file
if curPath != "" && cc.FilePath != curPath {
    pending = append(pending, pendingCommit{
        path:      curPath,
        lastReqID: prevReqID,
    })
}

if err := drainPending(cc.ReqID); err != nil {
    return err
}
```

Track `prevReqID` and `curPath`. After channel
closes (no more chunks), drain remaining pending
commits using `win.IsReleased`:

```go
// After flush() on channel close:
for len(pending) > 0 {
    head := pending[0]
    for !win.IsReleased(head.lastReqID) {
        // Invariant: flush() calls held.release()
        // which calls win.Release() for each chunk,
        // so this spin exits immediately in practice.
        // It is a safety net only.
        runtime.Gosched()
    }
    if err := sendCommit(stream, head.path); err != nil {
        return err
    }
    _ = reader.CloseFile(head.path)
    pending = pending[1:]
}
// Send commit for final file
if curPath != "" {
    if err := sendCommit(stream, curPath); err != nil {
        return err
    }
    _ = reader.CloseFile(curPath)
}
```

Note: use `runtime.Gosched()` instead of
`time.Sleep` to avoid import bloat. The window
drains very quickly in practice.

- [ ] **Step 4: Update `pipeline.go`**

Pass `reader` to `StageSendData`:

```go
// Stage 5: SendData
g.Go(func() error {
    return StageSendData(
        gctx, cfg, memRaw, win, stream,
        compressedCh, reader,
    )
})
```

- [ ] **Step 5: Run tests**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 6: Commit**

```bash
git commit -sm"feat(pipeline): add delayed commit and CloseFile in StageSendData"
```

---

## Task 6: Optional hashClient (nil = skip dedup)

**Files:**
- Modify: `internal/worker/pipeline/pipeline.go`

When `hashClient == nil`, `StageHash` and
`StageSendHash` are bypassed. Instead, `readCh` and
`zeroCh` are forwarded directly into `mismatchCh`
(treating all blocks as mismatches — always send).

Study `hashBatcher` in `stage_send_hash.go` to
understand how `zeroCh ZeroChunk` is wrapped as
`HashedChunk` (it uses `zeroHash = sha256.Sum256(zeroBuf)`
and creates a `HashedChunk` with nil Held since
window+mem were already released in StageRead).

- [ ] **Step 1: Write the failing test**

Add to `pipeline_test.go`:

```go
func TestPipeline_NilHashClient(t *testing.T) {
    // Same setup as existing tests but pass nil
    // hashClient. Expect pipeline to complete
    // successfully and send data.
    ctx := context.Background()

    data := make([]byte, 4*1024*1024)
    rand.Read(data)

    iter := &sliceIter{blocks: []pipeline.ChangeBlock{
        {FilePath: "/dev/block", Offset: 0, Len: int64(len(data))},
    }}
    reader := &mockDataReader{data: data}

    stream := &mockSyncStream{}
    cfg := pipeline.Config{}
    p := pipeline.New(cfg)

    err := p.Run(ctx, iter, reader, stream, nil)
    if err != nil {
        t.Fatal(err)
    }
    if len(stream.sent) == 0 {
        t.Fatal("expected data to be sent")
    }
}
```

Run:
```bash
make test
```

Expected: FAIL — nil hashClient panics.

- [ ] **Step 2: Add `forwardReadToMismatch` to `pipeline.go`**

```go
// forwardReadToMismatch is used when hashClient is nil.
// It forwards all ReadChunks and ZeroChunks directly
// to mismatchCh, treating every block as a mismatch.
func forwardReadToMismatch(
    ctx context.Context,
    memRaw *MemSemaphore,
    win *WindowSemaphore,
    readCh <-chan ReadChunk,
    zeroCh <-chan ZeroChunk,
    mismatchCh chan<- HashedChunk,
) error {
    for readCh != nil || zeroCh != nil {
        select {
        case rc, ok := <-readCh:
            if !ok {
                readCh = nil
                continue
            }
            select {
            case mismatchCh <- HashedChunk{
                ReqID:    rc.ReqID,
                FilePath: rc.FilePath,
                Offset:   rc.Offset,
                Length:   int64(len(rc.Data)),
                Data:     rc.Data,
                Held:     rc.Held,
            }:
            case <-ctx.Done():
                rc.Held.release(memRaw, nil, win)
                return ctx.Err()
            }
        case zc, ok := <-zeroCh:
            if !ok {
                zeroCh = nil
                continue
            }
            // ZeroChunk has no Held (released in StageRead)
            select {
            case mismatchCh <- HashedChunk{
                ReqID:    zc.ReqID,
                FilePath: zc.FilePath,
                Offset:   zc.Offset,
                Length:   zc.Length,
                Data:     nil,
            }:
            case <-ctx.Done():
                return ctx.Err()
            }
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    return nil
}
```

- [ ] **Step 3: Wire nil hashClient in `pipeline.Run()`**

```go
if hashClient == nil {
    // Skip hash stages: forward all to mismatchCh
    g.Go(func() error {
        defer close(mismatchCh)
        return forwardReadToMismatch(
            gctx, memRaw, win,
            readCh, zeroCh, mismatchCh,
        )
    })
} else {
    // Stage 2: Hash
    g.Go(func() error {
        defer close(hashedCh)
        return StageHash(gctx, cfg, readCh, hashedCh)
    })
    // Stage 3: SendHash
    g.Go(func() error {
        defer close(mismatchCh)
        return StageSendHash(
            gctx, cfg, memRaw, win, hashClient,
            hashedCh, zeroCh, mismatchCh,
        )
    })
}
```

When nil: `hashedCh` is never created (save alloc).

- [ ] **Step 4: Run tests**

```bash
make test
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git commit -sm"feat(pipeline): support nil hashClient to skip hash dedup stages"
```

---

## Task 7: CephFS BlockIterator

**Files:**
- Create: `internal/worker/cephfs/block_iterator.go`
- Create: `internal/worker/cephfs/block_iterator_test.go`

No build tag — uses a local `fileDiffIterator`
interface, not CGO types directly.
`cephfs/source.go` (which has `ceph_preview`) bridges
the CGO boundary when constructing the iterator.

- [ ] **Step 1: Write the failing test**

Create `block_iterator_test.go`:

```go
package cephfs

import (
    "testing"

    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

type mockFileDiffIter struct {
    blocks []changedBlock
    idx    int
    closed bool
}

func (m *mockFileDiffIter) More() bool {
    return m.idx < len(m.blocks)
}
func (m *mockFileDiffIter) Read() ([]changedBlock, error) {
    if m.idx >= len(m.blocks) {
        return nil, nil
    }
    b := []changedBlock{m.blocks[m.idx]}
    m.idx++
    return b, nil
}
func (m *mockFileDiffIter) Close() error {
    m.closed = true
    return nil
}

func TestCephFSBlockIterator_Empty(t *testing.T) {
    iter := NewCephFSBlockIterator(nil, nil)
    cb, ok := iter.Next()
    if ok || cb != nil {
        t.Fatal("expected empty iterator")
    }
    _ = iter.Close()
}

func TestCephFSBlockIterator_TwoFiles(t *testing.T) {
    fileA := &mockFileDiffIter{
        blocks: []changedBlock{
            {Offset: 0, Len: 4096},
            {Offset: 4096, Len: 4096},
        },
    }
    fileB := &mockFileDiffIter{
        blocks: []changedBlock{
            {Offset: 0, Len: 8192},
        },
    }
    iters := map[string]*mockFileDiffIter{
        "a.txt": fileA, "b.txt": fileB,
    }

    newIter := func(path string) (
        fileDiffIterator, error,
    ) {
        return iters[path], nil
    }

    iter := NewCephFSBlockIterator(
        newIter, []string{"a.txt", "b.txt"},
    )

    var got []*pipeline.ChangeBlock
    for {
        cb, ok := iter.Next()
        if !ok {
            break
        }
        got = append(got, cb)
    }

    if len(got) != 3 {
        t.Fatalf("expected 3 blocks, got %d", len(got))
    }
    if got[0].FilePath != "a.txt" {
        t.Errorf("expected a.txt, got %s", got[0].FilePath)
    }
    if got[2].FilePath != "b.txt" {
        t.Errorf("expected b.txt, got %s", got[2].FilePath)
    }
    _ = iter.Close()
    if !fileA.closed {
        t.Error("fileA iterator not closed")
    }
}
```

Run:
```bash
make test
```

Expected: compile error — types not defined.

- [ ] **Step 2: Create `block_iterator.go`**

```go
package cephfs

import (
    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

// fileDiffIterator abstracts go-ceph's per-file
// block diff iterator without importing CGO types.
// cephfs/source.go (with ceph_preview) wraps the
// concrete *ceph.BlockDiffIterator in this interface.
type fileDiffIterator interface {
    More() bool
    Read() ([]changedBlock, error)
    Close() error
}

// changedBlock mirrors ceph.ChangeBlock without CGO.
type changedBlock struct {
    Offset uint64
    Len    uint64
}

// CephFSBlockIterator implements pipeline.BlockIterator.
// It flattens block diffs across all large changed files.
type CephFSBlockIterator struct {
    newIter func(relPath string) (
        fileDiffIterator, error,
    )
    files   []string
    fileIdx int
    curIter fileDiffIterator
    curFile string
    buf     []changedBlock
    bufIdx  int
}

// NewCephFSBlockIterator creates an iterator over the
// given file paths. newIter is called once per file to
// open its block diff iterator. newIter may be nil only
// when files is also nil (empty case).
func NewCephFSBlockIterator(
    newIter func(string) (fileDiffIterator, error),
    files []string,
) *CephFSBlockIterator {
    return &CephFSBlockIterator{
        newIter: newIter,
        files:   files,
    }
}

// Next returns the next changed block, or (nil, false)
// when exhausted.
func (it *CephFSBlockIterator) Next() (
    *pipeline.ChangeBlock, bool,
) {
    for {
        // Drain buffer for current file.
        if it.bufIdx < len(it.buf) {
            b := it.buf[it.bufIdx]
            it.bufIdx++
            return &pipeline.ChangeBlock{
                FilePath: it.curFile,
                Offset:   int64(b.Offset), //nolint:gosec
                Len:      int64(b.Len),    //nolint:gosec
            }, true
        }

        // Try to read more from current iterator.
        if it.curIter != nil && it.curIter.More() {
            blocks, err := it.curIter.Read()
            if err != nil || len(blocks) == 0 {
                it.closeCurrentIter()
            } else {
                it.buf = blocks
                it.bufIdx = 0
                continue
            }
        }

        // Advance to next file.
        if it.fileIdx >= len(it.files) {
            return nil, false
        }
        it.closeCurrentIter()
        it.curFile = it.files[it.fileIdx]
        it.fileIdx++

        iter, err := it.newIter(it.curFile)
        if err != nil {
            // Skip files that cannot be opened.
            continue
        }
        it.curIter = iter
    }
}

func (it *CephFSBlockIterator) closeCurrentIter() {
    if it.curIter != nil {
        _ = it.curIter.Close()
        it.curIter = nil
        it.buf = nil
        it.bufIdx = 0
    }
}

// Close releases any open iterator.
func (it *CephFSBlockIterator) Close() error {
    it.closeCurrentIter()
    return nil
}

var _ pipeline.BlockIterator = (*CephFSBlockIterator)(nil)
```

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git commit -sm"feat(cephfs): add CephFSBlockIterator for pipeline integration"
```

---

## Task 8: CephFS DataReader

**Files:**
- Create: `internal/worker/cephfs/cephfs_reader.go`
- Create: `internal/worker/cephfs/cephfs_reader_test.go`

No build tag — pure Go, uses `os` only.

- [ ] **Step 1: Write the failing test**

Create `cephfs_reader_test.go`:

```go
package cephfs

import (
    "os"
    "path/filepath"
    "testing"
)

func TestCephFSReader_ReadAt(t *testing.T) {
    dir := t.TempDir()
    content := []byte("hello world data block!")
    path := filepath.Join(dir, "testfile.bin")
    _ = os.WriteFile(path, content, 0600)

    // Override DataMountPath for test by using
    // a reader that prefixes dir instead of
    // common.DataMountPath (test via direct path
    // injection or sub-package test)
    //
    // Simplest: use an absolute path as filePath
    // and modify CephFSReader to accept a base dir.
    r := newCephFSReader(dir)

    data, err := r.ReadAt("testfile.bin", 0,
        int64(len(content)))
    if err != nil {
        t.Fatal(err)
    }
    if string(data) != string(content) {
        t.Errorf("expected %q, got %q",
            content, data)
    }

    _ = r.CloseFile("testfile.bin")
    _ = r.Close()
}
```

Note: `CephFSReader` will accept a `baseDir` so it
can be tested without mounting CephFS.

Run:
```bash
make test
```

Expected: compile error.

- [ ] **Step 2: Create `cephfs_reader.go`**

```go
package cephfs

import (
    "fmt"
    "io"
    "os"
    "path/filepath"
    "sync"

    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

// CephFSReader implements pipeline.DataReader for
// CephFS files mounted under baseDir.
// Caches the most recently used file handle.
// CloseFile releases the handle after a CommitRequest.
type CephFSReader struct {
    baseDir  string
    mu       sync.Mutex
    openPath string
    openFile *os.File
}

// newCephFSReader creates a reader rooted at baseDir.
// Production code passes common.DataMountPath.
func newCephFSReader(baseDir string) *CephFSReader {
    return &CephFSReader{baseDir: baseDir}
}

// NewCephFSReader creates a reader for production use
// (rooted at common.DataMountPath).
func NewCephFSReader() *CephFSReader {
    return newCephFSReader(common.DataMountPath)
}

// ReadAt opens (or reuses) the file at
// baseDir/filePath and reads length bytes at offset.
func (r *CephFSReader) ReadAt(
    filePath string, offset, length int64,
) ([]byte, error) {
    r.mu.Lock()
    defer r.mu.Unlock()

    if r.openPath != filePath {
        if r.openFile != nil {
            _ = r.openFile.Close()
            r.openFile = nil
        }
        full := filepath.Join(r.baseDir, filePath)
        f, err := os.Open(full) //nolint:gosec // G304
        if err != nil {
            return nil, fmt.Errorf(
                "open %s: %w", full, err,
            )
        }
        r.openPath = filePath
        r.openFile = f
    }

    data := make([]byte, length)
    n, err := r.openFile.ReadAt(data, offset)
    if err != nil && err != io.EOF {
        return nil, fmt.Errorf(
            "pread %s at %d: %w",
            filePath, offset, err,
        )
    }
    return data[:n], nil
}

// CloseFile releases the open handle for filePath.
// Called by StageSendData after sending a commit.
func (r *CephFSReader) CloseFile(
    filePath string,
) error {
    r.mu.Lock()
    defer r.mu.Unlock()
    if r.openPath == filePath && r.openFile != nil {
        err := r.openFile.Close()
        r.openFile = nil
        r.openPath = ""
        return err
    }
    return nil
}

// Close releases any cached file handle.
func (r *CephFSReader) Close() error {
    r.mu.Lock()
    defer r.mu.Unlock()
    if r.openFile != nil {
        err := r.openFile.Close()
        r.openFile = nil
        r.openPath = ""
        return err
    }
    return nil
}

var _ pipeline.DataReader = (*CephFSReader)(nil)
```

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git commit -sm"feat(cephfs): add CephFSReader implementing pipeline.DataReader"
```

---

## Task 9: CephFS HashService

**Files:**
- Create: `internal/worker/cephfs/hash_server.go`
- Create: `internal/worker/cephfs/hash_server_test.go`

No build tag — pure Go, uses `os` + `crypto/sha256`.

- [ ] **Step 1: Write the failing test**

Create `hash_server_test.go`:

```go
package cephfs

import (
    "context"
    "crypto/sha256"
    "os"
    "path/filepath"
    "testing"

    "github.com/go-logr/logr"

    apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func TestCephFSHashServer_Mismatch(t *testing.T) {
    dir := t.TempDir()
    content := []byte("block content here")
    path := filepath.Join(dir, "file.bin")
    _ = os.WriteFile(path, content, 0600)

    srv := &CephFSHashServer{
        logger:  logr.Discard(),
        baseDir: dir,
    }

    wrongHash := [32]byte{0xFF}
    req := &apiv1.HashBatchRequest{
        Hashes: []*apiv1.BlockHash{
            {
                RequestId: 42,
                FilePath:  "file.bin",
                Offset:    0,
                Length:    uint64(len(content)),
                Sha256:    wrongHash[:],
            },
        },
    }

    resp, err := srv.CompareHashes(context.Background(), req)
    if err != nil {
        t.Fatal(err)
    }
    if len(resp.MismatchedIds) != 1 ||
        resp.MismatchedIds[0] != 42 {
        t.Errorf("expected reqID 42 mismatched")
    }
}

func TestCephFSHashServer_Match(t *testing.T) {
    dir := t.TempDir()
    content := []byte("block content here")
    path := filepath.Join(dir, "file.bin")
    _ = os.WriteFile(path, content, 0600)

    srv := &CephFSHashServer{
        logger:  logr.Discard(),
        baseDir: dir,
    }

    h := sha256.Sum256(content)
    req := &apiv1.HashBatchRequest{
        Hashes: []*apiv1.BlockHash{
            {
                RequestId: 7,
                FilePath:  "file.bin",
                Offset:    0,
                Length:    uint64(len(content)),
                Sha256:    h[:],
            },
        },
    }

    resp, err := srv.CompareHashes(context.Background(), req)
    if err != nil {
        t.Fatal(err)
    }
    if len(resp.MismatchedIds) != 0 {
        t.Errorf("expected no mismatches for identical hash")
    }
}

func TestCephFSHashServer_MissingFile(t *testing.T) {
    dir := t.TempDir()
    srv := &CephFSHashServer{
        logger:  logr.Discard(),
        baseDir: dir,
    }
    req := &apiv1.HashBatchRequest{
        Hashes: []*apiv1.BlockHash{
            {RequestId: 1, FilePath: "noexist.bin",
             Offset: 0, Length: 100},
        },
    }
    resp, err := srv.CompareHashes(context.Background(), req)
    if err != nil {
        t.Fatal(err)
    }
    if len(resp.MismatchedIds) != 1 {
        t.Error("missing file should be a mismatch")
    }
}
```

Run:
```bash
make test
```

Expected: compile error.

- [ ] **Step 2: Create `hash_server.go`**

```go
package cephfs

import (
    "context"
    "crypto/sha256"
    "fmt"
    "io"
    "os"
    "path/filepath"

    "github.com/go-logr/logr"

    apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
    "github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// CephFSHashServer implements HashServiceServer for
// CephFS file-based hash comparison.
// For each requested block it reads the destination
// file at the given offset and compares SHA-256.
type CephFSHashServer struct {
    apiv1.UnimplementedHashServiceServer
    logger  logr.Logger
    baseDir string
}

// NewCephFSHashServer creates a hash server rooted at
// common.DataMountPath (production use).
func NewCephFSHashServer(logger logr.Logger) *CephFSHashServer {
    return &CephFSHashServer{
        logger:  logger,
        baseDir: common.DataMountPath,
    }
}

// CompareHashes reads destination file blocks,
// computes SHA-256, and returns mismatched req IDs.
func (s *CephFSHashServer) CompareHashes(
    _ context.Context,
    req *apiv1.HashBatchRequest,
) (*apiv1.HashBatchResponse, error) {
    resp := &apiv1.HashBatchResponse{}

    for _, bh := range req.Hashes {
        full := filepath.Join(s.baseDir, bh.FilePath)
        data, err := readAt(
            full, int64(bh.Offset), //nolint:gosec
            int64(bh.Length),       //nolint:gosec
        )
        if err != nil {
            // File not present on destination =
            // must send all its blocks.
            resp.MismatchedIds = append(
                resp.MismatchedIds, bh.RequestId,
            )
            continue
        }

        localHash := sha256.Sum256(data)
        if len(bh.Sha256) != 32 ||
            localHash != ([32]byte)(bh.Sha256) {
            resp.MismatchedIds = append(
                resp.MismatchedIds, bh.RequestId,
            )
        }
    }

    s.logger.V(1).Info(
        "Hash comparison",
        "total", len(req.Hashes),
        "mismatched", len(resp.MismatchedIds),
    )
    return resp, nil
}

func readAt(
    path string, offset, length int64,
) ([]byte, error) {
    f, err := os.Open(path) //nolint:gosec // G304
    if err != nil {
        return nil, fmt.Errorf(
            "open %s: %w", path, err,
        )
    }
    defer func() { _ = f.Close() }()

    data := make([]byte, length)
    n, err := f.ReadAt(data, offset)
    if err != nil && err != io.EOF {
        return nil, fmt.Errorf(
            "pread %s at %d: %w", path, offset, err,
        )
    }
    return data[:n], nil
}
```

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git commit -sm"feat(cephfs): add CephFSHashServer for block-level hash dedup"
```

---

## Task 10: CephFS destination — use RunWithHash

**Files:**
- Modify: `internal/worker/cephfs/destination.go`

`BaseDestinationWorker.RunWithHash` already exists in
`internal/worker/common/base_dest.go` (line 51).
Change the CephFS `DestinationWorker.Run()` to use it.

- [ ] **Step 1: Update `destination.go` `Run()` method**

```go
// Before (lines 57-67)
func (w *DestinationWorker) Run(
    ctx context.Context,
) error {
    dataServer := &DataServer{
        logger: w.Logger,
    }
    return w.BaseDestinationWorker.Run(
        ctx, dataServer,
    )
}

// After
func (w *DestinationWorker) Run(
    ctx context.Context,
) error {
    dataServer := &DataServer{
        logger: w.Logger,
    }
    hashServer := NewCephFSHashServer(w.Logger)
    return w.BaseDestinationWorker.RunWithHash(
        ctx, dataServer, hashServer,
    )
}
```

- [ ] **Step 2: Run tests**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 3: Commit**

```bash
git commit -sm"feat(cephfs): register HashService on destination gRPC server"
```

---

## Task 11: Refactor CephFS source — use pipeline

**Files:**
- Modify: `internal/worker/cephfs/source.go`

### What changes

1. `syncState`: remove `blockChan` and `nextReqID`.
2. `runStatelessSync`: add `conn *grpc.ClientConn`
   param, call `runSnapdiffBlockPipeline` for large
   files instead of `StreamBlocks`.
3. `runSnapdiffSync`: pass `conn` to `runStatelessSync`.
4. `initSyncState`: remove `blockChan` init.
5. Remove `streamBlockDiff` function entirely.
6. `processFile`: for large files, append to a
   `largeFiles []string` slice (passed in) instead of
   calling `streamBlockDiff`.
7. Add `collectChangedEntries` helper.
8. Add `runSnapdiffBlockPipeline` method.

The `ceph.BlockDiffIterator` is a CGO type. We must
bridge it to `fileDiffIterator` in a file that has the
`ceph_preview` build context (i.e., `source.go` which
already imports `go-ceph`).

Add this bridge adapter in `source.go`:

```go
// cephBlockDiffAdapter bridges *ceph.BlockDiffIterator
// to the cephfs-package fileDiffIterator interface.
type cephBlockDiffAdapter struct {
    inner *ceph.BlockDiffIterator
}

func (a *cephBlockDiffAdapter) More() bool {
    return a.inner.More()
}
func (a *cephBlockDiffAdapter) Read() (
    []changedBlock, error,
) {
    cbs, err := a.inner.Read()
    if err != nil || cbs == nil {
        return nil, err
    }
    result := make([]changedBlock, len(cbs.ChangedBlocks))
    for i, b := range cbs.ChangedBlocks {
        result[i] = changedBlock{
            Offset: b.Offset, Len: b.Len,
        }
    }
    return result, nil
}
func (a *cephBlockDiffAdapter) Close() error {
    return a.inner.Close()
}
```

- [ ] **Step 1: Write the failing test**

Add a compile-only integration check. Since the
source involves CGO, unit testing is limited. Add a
simple test that verifies `runSnapdiffBlockPipeline`
signature exists:

```go
// In a *_test.go file with appropriate build context:
// Just verify the function signature compiles.
var _ = (*SourceWorker).runSnapdiffBlockPipeline
```

Run:
```bash
make test
```

Expected: compile error — `runSnapdiffBlockPipeline`
not defined.

- [ ] **Step 2: Add `collectChangedEntries`**

```go
// collectChangedEntries walks the snapdiff and returns
// three slices: large files (for pipeline), small
// files (for rsync), and deleted paths.
func (w *SourceWorker) collectChangedEntries(
    ctx context.Context,
    state *syncState,
) (large, small, deleted []string, err error) {
    dirChan, errChan := w.walkAndStreamDirectories()

    for dirPath := range dirChan {
        select {
        case <-ctx.Done():
            return nil, nil, nil, ctx.Err()
        default:
        }

        iterator, iterErr :=
            state.differ.NewSnapDiffIterator(dirPath)
        if iterErr != nil {
            return nil, nil, nil, fmt.Errorf(
                "snap diff iterator %s: %w",
                dirPath, iterErr,
            )
        }

        for {
            entry, readErr := iterator.Read()
            if readErr != nil {
                _ = iterator.Close()
                return nil, nil, nil, readErr
            }
            if entry == nil {
                break
            }
            entryName := entry.DirEntry.Name()
            entryPath := filepath.Join(
                dirPath, entryName,
            )

            if entry.DirEntry.DType() !=
                cephfs.DTypeReg {
                fullP := filepath.Join(
                    common.DataMountPath, entryPath,
                )
                if _, statErr := os.Stat(fullP);
                    os.IsNotExist(statErr) {
                    deleted = append(deleted, entryPath)
                } else {
                    small = append(small, entryPath)
                }
                continue
            }

            fullP := filepath.Join(
                common.DataMountPath, entryPath,
            )
            fi, statErr := os.Stat(fullP)
            if os.IsNotExist(statErr) {
                deleted = append(deleted, entryPath)
                continue
            } else if statErr != nil {
                _ = iterator.Close()
                return nil, nil, nil, statErr
            }

            if fi.Size() <= smallFileMaxSize {
                small = append(small, entryPath)
            } else {
                large = append(large, entryPath)
                // Also queue for metadata rsync
                // (handled via small=false rsync)
            }
        }
        _ = iterator.Close()
    }

    if walkErr := <-errChan; walkErr != nil {
        return nil, nil, nil, walkErr
    }
    return large, small, deleted, nil
}
```

- [ ] **Step 3: Add `runSnapdiffBlockPipeline`**

```go
// runSnapdiffBlockPipeline runs the 5-stage pipeline
// for all large changed files.
func (w *SourceWorker) runSnapdiffBlockPipeline(
    ctx context.Context,
    differ *ceph.SnapshotDiffer,
    conn *grpc.ClientConn,
    largeFiles []string,
) error {
    if len(largeFiles) == 0 {
        return nil
    }

    newIter := func(relPath string) (
        fileDiffIterator, error,
    ) {
        it, err := differ.NewBlockDiffIterator(relPath)
        if err != nil {
            return nil, err
        }
        return &cephBlockDiffAdapter{inner: it}, nil
    }

    iter := NewCephFSBlockIterator(newIter, largeFiles)
    defer func() { _ = iter.Close() }()

    reader := NewCephFSReader()
    defer func() { _ = reader.Close() }()

    hashClient := apiv1.NewHashServiceClient(conn)
    dataClient := apiv1.NewDataServiceClient(conn)
    stream, err := dataClient.Sync(ctx)
    if err != nil {
        return fmt.Errorf(
            "failed to create sync stream: %w", err,
        )
    }

    cfg := pipeline.Config{ReadWorkers: 2}
    p := pipeline.New(cfg)
    if err := p.Run(
        ctx, iter, reader, stream, hashClient,
    ); err != nil {
        return err
    }

    if _, err := stream.CloseAndRecv(); err != nil {
        return fmt.Errorf(
            "failed to close sync stream: %w", err,
        )
    }
    return nil
}
```

- [ ] **Step 4: Refactor `runStatelessSync`**

Change signature to accept `conn *grpc.ClientConn`:

```go
func (w *SourceWorker) runStatelessSync(
    ctx context.Context,
    conn *grpc.ClientConn,
    differ *ceph.SnapshotDiffer,
    dataClient apiv1.DataServiceClient,
) error {
    w.Logger.Info("Starting stateless snapshot sync")

    state := w.initSyncState(differ, dataClient)

    large, small, deleted, err :=
        w.collectChangedEntries(ctx, state)
    if err != nil {
        return fmt.Errorf(
            "collecting changed entries: %w", err,
        )
    }

    // Phase 1: block diff via pipeline (large files)
    if err := w.runSnapdiffBlockPipeline(
        ctx, differ, conn, large,
    ); err != nil {
        return fmt.Errorf(
            "block pipeline failed: %w", err,
        )
    }

    // Phase 2: rsync small files (content+metadata)
    if len(small) > 0 {
        if err := w.rsyncBatch(
            small, state.rsyncTarget, true,
        ); err != nil {
            return fmt.Errorf(
                "small file rsync: %w", err,
            )
        }
    }

    // Phase 3: rsync large file metadata only
    if len(large) > 0 {
        if err := w.rsyncBatch(
            large, state.rsyncTarget, false,
        ); err != nil {
            return fmt.Errorf(
                "large file meta rsync: %w", err,
            )
        }
    }

    // Phase 4: delete removed files/dirs
    for i := 0; i < len(deleted);
        i += deleteBatchSize {
        end := i + deleteBatchSize
        if end > len(deleted) {
            end = len(deleted)
        }
        if err := w.sendDeleteBatch(
            ctx, state, deleted[i:end],
        ); err != nil {
            return err
        }
    }

    // Phase 5: convergence rsync
    if err := w.rsyncConvergence(state); err != nil {
        return fmt.Errorf(
            "convergence rsync: %w", err,
        )
    }

    w.Logger.Info("Stateless sync completed")
    return nil
}
```

Update the call site in `runSnapdiffSync` (line 256):

```go
if err := w.runStatelessSync(
    ctx, conn, differ, dataClient,
); err != nil {
```

- [ ] **Step 5: Remove old code**

Delete:
- `streamBlockDiff` function (lines 820-876)
- `blockChan` field from `syncState` (line 72)
- `nextReqID` field from `syncState` (line 74)
- `blockChan` init in `initSyncState` (line 677)
- `StreamBlocks` goroutine from old `runStatelessSync`
- Old errgroup-based `runStatelessSync` body
- Unused imports: `"golang.org/x/sync/errgroup"`
  if no longer needed in source.go
- `"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"`
  if `StreamBlocks` / `BlockEntry` are the only uses
  (keep if `common.SignalDone`, `common.ReadMountedCredentials`,
  `common.DataMountPath` are still used)

Add import:
```go
"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
```

- [ ] **Step 6: Run tests**

```bash
make test
```

Expected: all tests pass.

- [ ] **Step 7: Commit**

```bash
git commit -sm"feat(cephfs): replace streamBlockDiff with shared pipeline"
```

---

## Task 12: Delete `common/streamer.go`

**Files:**
- Delete: `internal/worker/common/streamer.go`

- [ ] **Step 1: Verify no remaining callers**

```bash
grep -r "StreamBlocks\|ReadBlockEntry\|BlockEntry\b" \
  internal/ --include="*.go"
```

Expected: **no matches**. If any match is found,
do NOT proceed — go back and fix the caller first.

```bash
grep -r "worker/common.*streamer\|from.*streamer" \
  internal/ --include="*.go"
```

Also expected: no matches.

- [ ] **Step 2: Delete the file**

```bash
git rm internal/worker/common/streamer.go
# Also remove its test if it exists:
git rm internal/worker/common/streamer_test.go 2>/dev/null || true
```

- [ ] **Step 3: Run tests**

```bash
make test
```

Expected: all pass.

- [ ] **Step 4: Verify pipeline import**

```bash
grep -r "worker/rbd/pipeline" internal/ --include="*.go"
```

Expected: no output.

- [ ] **Step 5: Commit**

```bash
git commit -sm"refactor: remove common.StreamBlocks replaced by pipeline"
```

---

## Task 13: Build verification

- [ ] **Step 1: Run unit tests**

```bash
make test
```

Expected: PASS.

- [ ] **Step 2: Run linter**

```bash
make lint
```

Expected: no new lint errors.

- [ ] **Step 3: Docker build (mover image)**

```bash
make docker-build-mover
```

Expected: successful image build with CGO and
`ceph_preview` build tag.
