# RBD Concurrent Transfer Pipeline Design

**Date:** 2026-03-21
**Status:** Draft
**Scope:** `internal/worker/rbd/`,
`internal/worker/common/`, `internal/proto/api/v1/`

## 1. Problem Statement

The RBD source worker (`internal/worker/rbd/source.go`)
uses a two-goroutine model that cannot saturate
cross-cluster links above 1 Gbps:

1. **No deduplication** -- every changed block is sent
   even if the destination already has identical data.
   With typical incremental syncs showing 70% match
   rates, 70% of transferred bytes are wasted.
2. **No compression** -- raw 4 MB chunks are sent
   over the wire. LZ4 can halve payload at 3+ GB/s
   with negligible CPU cost.
3. **No verification** -- silent bitrot on the
   destination (partial pwrite, hardware offload
   bypass) is undetectable. The source has no way
   to confirm the destination wrote correct data.
4. **Unbounded memory** -- `produceBlocks()` reads
   blocks into `blockChan` (buffered 64). At 4 MB
   chunks this is 256 MB of in-flight data with no
   back-pressure if the network is slower than disk.
5. **Single-threaded read** -- one goroutine drives
   `RBDBlockDiffIterator`, bottlenecked by
   single-OSD latency rather than aggregate IOPS.

### Current Code Path

```
SourceWorker.Sync()           // rbd/source.go
  resolveSourceConfig()       // env vars + creds
  resolveParentImage()        // full or incremental
  NewRBDBlockDiffIterator()   // ceph/rbd.go
  errgroup:
    produceBlocks()           // rbd/source.go
      iter.Next() -> ReadBlockEntry() -> blockChan
    StreamBlocks()            // common/streamer.go
      accumulate -> SendBlockWrite() -> gRPC stream
  closeAndSignalDone()        // CommitRequest + Done
```

## 2. Current Architecture

### 2.1 Source Data Flow

```
RBDBlockDiffIterator.Next()        ceph/rbd.go
  |  ChangeBlock{Offset, Len}
  v
produceBlocks()                    rbd/source.go
  ReadBlockEntry(device, off, len) common/streamer.go
    |  BlockEntry{ReqID, Offset, Len, IsZero, Data}
    v
  blockChan  (buffered 64)
    |
    v
StreamBlocks()                     common/streamer.go
  accumulate until >= 2 MB
    |  WriteRequest{path, []ChangedBlock}
    v
  SendBlockWrite()                 common/client.go
    stream.Send(SyncRequest_Write)
    |
    v
  gRPC Sync stream  ----network---->  destination
```

### 2.2 Destination Data Flow

```
RBDDataServer.Sync()               rbd/destination.go
  stream.Recv()
    |  SyncRequest
    v
  oneof:
    WriteRequest -> writeBlocks()
      for each ChangedBlock:
        WriteAt(data, offset) on /dev/block
    CommitRequest -> device.Sync()
```

### 2.3 Current Proto Contract

`internal/proto/api/v1/data.proto`:

```protobuf
service DataService {
  rpc Sync(stream SyncRequest) returns (SyncResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}

message SyncRequest {
  oneof operation {
    WriteRequest write = 1;
    CommitRequest commit = 3;
  }
}

message WriteRequest {
  string path = 1;
  repeated ChangedBlock blocks = 2;
}

message ChangedBlock {
  uint64 offset = 1;
  uint64 length = 2;
  bytes data = 3;
  bool is_zero = 4;
}
```

### 2.4 Key Types Reference

| Type | Package | File |
|------|---------|------|
| `SourceWorker` | `rbd` | `rbd/source.go` |
| `RBDDataServer` | `rbd` | `rbd/destination.go` |
| `BaseSourceWorker` | `common` | `common/base_source.go` |
| `BaseDestinationWorker` | `common` | `common/base_dest.go` |
| `BlockEntry` | `common` | `common/streamer.go` |
| `RBDBlockDiffIterator` | `ceph` | `ceph/rbd.go` |
| `ChangeBlock` | `ceph` | `ceph/rbd.go` |
| `ConnectToDestination` | `common` | `common/client.go` |
| `SignalDone` | `common` | `common/client.go` |
| `SendBlockWrite` | `common` | `common/client.go` |
| `StreamBlocks` | `common` | `common/streamer.go` |
| `RunDestinationServer` | `common` | `common/destination.go` |

All paths relative to `internal/worker/` unless
noted.

## 3. Pipeline Overview

### 3.1 Design Constraints

Three hard constraints govern every design decision:

1. **Memory must be bounded** regardless of RBD
   or network speed
2. **The spread of request IDs alive simultaneously
   must be bounded** so the receiver uses a
   fixed-size reassembly buffer
3. **Matched chunks release all resources at the
   earliest possible stage**

### 3.2 Six-Stage Architecture

```
Iterator (ceph/rbd.go RBDBlockDiffIterator)
  |  chan Chunk             (buf = ReadWorkers)
  v
StageRead x ReadWorkers
  |  chan ReadChunk         (buf = ReadWorkers)
  |  chan ZeroChunk         (buf = ReadWorkers)
  v
StageHash x HashWorkers
  |  chan HashedChunk       (buf = HashWorkers)
  v
StageSendHash [batcher x1 + senders x HSW]
  |  chan []HashedChunk     (buf = HSW x 2)
  |  matched -> release mem_raw + win immediately
  |  chan HashedChunk       (buf = HashWorkers)
  v
StageCompress x CompressWorkers
  |  chan CompressedChunk   (buf = CW x 2)
  v
StageSendData x DataSendWorkers
  |  release mem_compressed + win after ACK
  |  chan VerifyResult      (buf = VW x 4)
  v
StageVerify x 1
```

### 3.3 Stage Summary

| Stage | Workers | Bound By | Sems Held | Responsibility |
|-------|---------|----------|-----------|----------------|
| StageRead | ReadWorkers | RBD I/O | mem_raw + win | Acquire sems; pread from /dev/block; zero-block short-circuit |
| StageHash | HashWorkers | CPU | pass-through | SHA-256 of chunk bytes; emit HashedChunk |
| StageSendHash | 1 batcher + HSW | Network | mem_raw + win (hold RTT) | Batch hashes; RPC to receiver; release matched immediately |
| StageCompress | CompressWorkers | CPU | mem_raw -> mem_compressed | LZ4 in-place or Zstd double-buffer; mismatched only |
| StageSendData | DataSendWorkers | Network | mem_compressed + win | Batch and send compressed data; final release point |
| StageVerify | 1 | Metadata | none | Async post-write hash check; logs mismatches |

### 3.4 Zero-Block Short-Circuit

Zero blocks are detected in StageRead immediately
after pread (reuses `IsAllZero()` from
`common/util.go`). Both semaphores are released
and a `ZeroChunk` is emitted directly to the
StageSendHash batcher which merges it with the
HashedChunk stream using the canonical zero hash.
No buffer is forwarded downstream; the pool slot
is recycled before any channel hop.

The zero hash is `SHA-256(zero_buf of ChunkSize
bytes)`. This value must be identical at sender
and receiver. It is not the hash of an empty
string and it is not the null `[32]byte` sentinel.

## 4. Semaphore Design

### 4.1 Acquisition Order

Strict global ordering prevents deadlock. Every
goroutine that acquires more than one semaphore
must do so in this sequence:

```
mem_raw -> win -> mem_compressed
```

**Rationale:** each semaphore's Release depends on
a downstream stage completing. Acquisition must
follow data-flow order. `win` is acquired last
(relative to `mem_raw`) because an unreleased ID
pins `base` permanently; acquiring it as late as
possible minimises the window between acquisition
and the first channel send.

### 4.2 MemSemaphore (Weighted, Channel-per-Waiter)

Two independent pools:

| Property | mem_raw | mem_compressed |
|----------|---------|----------------|
| Acquired at | StageRead | StageCompress (Zstd) |
| Released at | StageSendHash (matched) or StageCompress (mismatched) | StageSendData after ACK |
| Covers | read -> hash -> hash RTT -> compress input | compress output -> send RTT |
| LZ4 | in-place shrink; no transfer to second pool | not used (0 capacity) |
| Zstd | held until compressed buffer allocated | holds compressed bytes through send |
| Wake | FIFO; prevents starvation of large chunks | FIFO |
| Spurious wakeups | Zero; each waiter owns a unique channel | Zero |

**Go type signature:**

```go
// MemSemaphore is a weighted semaphore with
// channel-per-waiter wake discipline.
type MemSemaphore struct {
    capacity int64
    // ...
}

func NewMemSemaphore(capacity int64) *MemSemaphore
func (s *MemSemaphore) Acquire(
    ctx context.Context, n int64,
) error
func (s *MemSemaphore) Release(n int64)
func (s *MemSemaphore) PartialRelease(n int64)
```

`PartialRelease` is used by LZ4 in-place to return
excess bytes after compression shrinks the buffer.

### 4.3 WindowSemaphore (ID-Aware Sliding Window)

#### Invariant

```
incomingReqID - min(in_flight_req_IDs) < 2 * MaxWindow
```

Bounds the **spread** of IDs alive in the pipeline,
not merely the count. The receiver allocates a
fixed reassembly buffer of exactly `2 * MaxWindow`
entries. No dynamic allocation is required on
either side.

#### reqID Contract

- reqIDs are dense, monotonically increasing
  integers starting at 0
- The Iterator is the sole assigner; no ID may
  be skipped or reused within a pipeline lifetime
- A gap in reqIDs permanently pins `base`; once
  `2 * MaxWindow` further IDs are issued the
  pipeline deadlocks silently
- Resume/checkpoint features must reset the
  WindowSemaphore to match the resumed base
  before issuing new IDs

#### Release and Base Advancement

```
Mark released[reqID % limit] = true
Scan forward from base:
  while released[base % limit] is true:
    clear slot, increment base
Walk sorted waiter list front-to-back:
  close channel of each waiter whose reqID
  satisfies reqID - base < limit
```

Multiple waiters may be unblocked in a single
Release call if `base` advances several steps.

**Go type signature:**

```go
type WindowSemaphore struct {
    maxWindow int
    // ...
}

func NewWindowSemaphore(
    maxWindow int,
) *WindowSemaphore
func (w *WindowSemaphore) Acquire(
    ctx context.Context, reqID uint64,
) error
func (w *WindowSemaphore) Release(reqID uint64)
func (w *WindowSemaphore) PressureSignal(
    threshold float64,
) <-chan struct{}
```

#### Pressure Signal

The WindowSemaphore exposes a
`PressureSignal(threshold)` channel. It fires when
in-flight count exceeds `threshold * MaxWindow`.
The StageSendHash batcher subscribes at
`threshold=0.75` as a flush trigger. This replaces
timer-driven flush entirely.

```go
case <-win.PressureSignal(0.75):
    if len(batch) > 0 { batcher.flush() }
```

## 5. Stage Definitions

### 5.1 StageRead

**Replaces:** `produceBlocks()` in `rbd/source.go`
and `ReadBlockEntry()` in `common/streamer.go`.

**Workers:** `ReadWorkers` goroutines (default 8).

**Flow per chunk:**

1. Receive `Chunk{ReqID, Offset, Length}` from
   Iterator channel
2. `memRaw.Acquire(ctx, ChunkSize)` -- blocks if
   memory budget exhausted
3. `win.Acquire(ctx, reqID)` -- blocks if window
   spread exceeded
4. `pread(device, offset, length)` -- ReadAt on
   `/dev/block`
5. If `IsAllZero(data)`:
   - Release `memRaw` and `win`
   - Emit `ZeroChunk` to StageSendHash directly
6. Else:
   - Emit `ReadChunk{reqID, data, held}` to
     StageHash channel

**Bound by:** RBD I/O latency. Cap at
`N_OSDs * 4` workers to avoid librbd queue
saturation.

### 5.2 StageHash

**New capability** -- no equivalent in current
codebase.

**Workers:** `HashWorkers` goroutines
(default `NumCPU / 2`).

**Flow:**

1. Receive `ReadChunk` from StageRead channel
2. Compute `sha256.Sum256(data)`
3. Emit `HashedChunk{reqID, data, hash, held}`
   to StageSendHash channel

**Bound by:** CPU. AVX-512 SHA-256 runs at
8-15 GB/s; 2 cores saturate a 2 GB/s source.
Half-CPU allocation leaves headroom for
CompressWorkers and OS.

Semaphores are passed through (not acquired or
released at this stage).

### 5.3 StageSendHash

**New capability** -- no equivalent in current
codebase. Requires new `HashService` proto RPC
(see Section 8).

**Architecture:** 1 batcher goroutine +
`HashSendWorkers` sender goroutines (default 2).

**Batcher flow:**

1. Collect `HashedChunk` entries into batch
2. Flush on any trigger (see Section 6)
3. Send batch to sender goroutines via channel

**Sender flow:**

1. Receive batch from batcher channel
2. Send `HashBatchRequest` RPC to destination
3. Destination returns `HashBatchResponse` with
   list of mismatched reqIDs
4. For each chunk in batch:
   - **Matched:** release `memRaw` and `win`
     immediately (chunk already exists at dest)
   - **Mismatched:** forward to StageCompress
     channel

**Key property:** Matched chunks skip compression
and data send entirely, releasing all resources
at the earliest possible point.

### 5.4 StageCompress

**New capability** -- no equivalent in current
codebase.

**Workers:** `CompressWorkers` goroutines
(default 1 for LZ4).

**Only processes mismatched chunks** from
StageSendHash.

#### LZ4 In-Place Strategy (Default)

1. Compress into the head of the same buffer;
   track `compressed_len`
2. `memRaw.PartialRelease(ChunkSize - compressed_len)`
3. No second pool; no double-buffer deadlock risk;
   zero extra allocation
4. `mem_compressed` pool capacity = 0 in Config

#### Zstd Double-Buffer Strategy

1. `memCompressed.Acquire(ctx, ChunkSize / ratio)`
2. Compress into new buffer
3. `memRaw.Release(ChunkSize)` -- release raw slot
4. Emit with compressed buffer under
   `mem_compressed` ownership

#### CompressWorkers Formula

```
CompressWorkers = ceil(lambda * (1 - MatchRate)
                       * t_compress)
```

#### Algorithm Selection

| Algorithm | Compress | Decompress | Strategy | mem_compressed |
|-----------|----------|------------|----------|----------------|
| LZ4 | 3-5 GB/s | 10-15 GB/s | In-place | Not used |
| Zstd L1 | 400-600 MB/s | 1.5-2.5 GB/s | Double-buffer | Required |
| Zstd L3 | 250-350 MB/s | 1.5-2.5 GB/s | Double-buffer | Required |
| Zstd L9+ | 30-80 MB/s | 1.5-2.5 GB/s | Double-buffer | Required |

### 5.5 StageSendData

**Replaces:** `StreamBlocks()` in
`common/streamer.go` and `SendBlockWrite()` in
`common/client.go` for the RBD path.

**Workers:** `DataSendWorkers` goroutines
(default 4).

**Flow:**

1. Receive `CompressedChunk` from StageCompress
2. Batch by count/bytes (see Section 6)
3. Send `WriteRequest` with compressed
   `ChangedBlock` entries via gRPC Sync stream
4. On ACK: release held memory semaphore + `win`
   (LZ4: remaining `mem_raw`; Zstd: `mem_compressed`)
5. Emit `VerifyResult{reqID, hash}` to
   StageVerify channel

**Key change from current:** ACK is on pwrite
complete at the destination, not on receive.
This prevents unbounded receiver memory during
destination Ceph degradation.

### 5.6 StageVerify

**New capability** -- no equivalent in current
codebase.

**Workers:** 1 goroutine (VerifyWorkers).

**Flow:**

1. Receive `VerifyResult` from destination after
   each pwrite completes
2. Destination computes SHA-256 of written bytes
   and returns it alongside the reqID
3. Compare against the original `HashedChunk.Hash`
4. On mismatch: log with reqID and offset;
   optionally trigger targeted retry via new
   reqID from Iterator

**Properties:**

- Holds no semaphore; memory released before
  VerifyResult arrives
- Operates on metadata only: reqID (8 bytes) +
  hash (32 bytes) per result
- Async: StageSendData does not wait for verify
  before releasing `win`
- Detects: silent bitrot in destination Ceph,
  hardware offload checksum bypasses, partial
  pwrite bugs

## 6. Flush Triggers (Timer-Free)

The pipeline contains no timer-driven flush paths.
All flushes are triggered by structural conditions.

| Trigger | Condition | Stage |
|---------|-----------|-------|
| Count | `len(batch) >= MaxCount` | Hash + Data batchers |
| Bytes | accumulated bytes >= MaxBytes | Hash + Data batchers |
| Channel close | input channel nil (all upstream exited) | Both batchers (tail flush) |
| Window pressure | `win.PressureSignal(0.75)` fires | Hash batcher only |

### Joint Constraint (must hold at config time)

```
HashBatchMaxCount < lambda_min * MaxWindow * RTT * 0.8
```

This ensures that at minimum throughput, hash
batches never accumulate enough chunks to exhaust
the window before a flush fires. `lambda_min` is
the sustained chunk rate at the slowest expected
read speed.

## 7. Proto Changes

### 7.1 New: `internal/proto/api/v1/hash.proto`

```protobuf
syntax = "proto3";
package api.v1;

option go_package =
    "github.com/RamenDR/ceph-volsync-plugin"
    "/internal/proto/api/v1";

// HashService allows the source to check which
// chunks the destination already has, avoiding
// redundant data transfer.
service HashService {
  rpc CompareHashes(HashBatchRequest)
      returns (HashBatchResponse);
}

message BlockHash {
  uint64 request_id = 1;
  uint64 offset = 2;
  uint64 length = 3;
  bytes sha256 = 4;   // 32 bytes
}

message HashBatchRequest {
  repeated BlockHash hashes = 1;
}

message HashBatchResponse {
  // reqIDs of chunks that do NOT match the
  // destination's data (must be sent).
  repeated uint64 mismatched_ids = 1;
}
```

### 7.2 Extended: `internal/proto/api/v1/data.proto`

New enum:

```protobuf
enum CompressionAlgo {
  COMPRESSION_NONE = 0;
  COMPRESSION_LZ4 = 1;
  COMPRESSION_ZSTD = 2;
}
```

Extended `ChangedBlock` (fields 5-7 are additive,
non-breaking):

```protobuf
message ChangedBlock {
  uint64 offset = 1;
  uint64 length = 2;
  bytes data = 3;
  bool is_zero = 4;
  // Pipeline additions:
  uint64 request_id = 5;
  CompressionAlgo compression = 6;
  uint64 uncompressed_length = 7;
}
```

New `Verify` RPC on `DataService`:

```protobuf
service DataService {
  rpc Sync(stream SyncRequest)
      returns (SyncResponse);
  rpc Delete(DeleteRequest)
      returns (DeleteResponse);
  // Pipeline addition:
  rpc Verify(VerifyRequest)
      returns (VerifyResponse);
}

message VerifyRequest {
  repeated BlockHash expected = 1;
}

message VerifyResponse {
  repeated VerifyResult results = 1;
}

message VerifyResult {
  uint64 request_id = 1;
  bool matched = 2;
  bytes actual_sha256 = 3;  // for diagnostics
}
```

### 7.3 Wire Compatibility

All changes are additive per proto3 semantics:

- New fields on `ChangedBlock` (5-7) default to
  zero/empty for old senders
- New `CompressionAlgo` enum defaults to
  `COMPRESSION_NONE` (field 0)
- New `HashService` is a separate service;
  old destinations simply don't register it
- New `Verify` RPC returns `UNIMPLEMENTED` on
  old destinations; source can detect via
  `VersionService` handshake

## 8. Receiver (Destination) Changes

### 8.1 Hash Comparison Server

New `HashServiceServer` implementation in
`internal/worker/rbd/`:

1. Receive `HashBatchRequest` with block hashes
2. For each `BlockHash`:
   - `pread(device, offset, length)` from local
     `/dev/block`
   - Compute `sha256.Sum256(data)`
   - Compare against received `sha256`
3. Return `HashBatchResponse` with mismatched
   reqIDs

This requires the destination to have read access
to `/dev/block` (already available -- current
`RBDDataServer` opens the device).

### 8.2 Reassembly Buffer

The destination allocates a fixed reassembly
buffer of `2 * MaxWindow` entries. Out-of-order
compressed blocks are held until their offset
ordering allows sequential write to `/dev/block`.

- No dynamic allocation; size fixed at startup
- MaxWindow communicated via version handshake
  or config exchange

### 8.3 Decompression

Modified `writeBlocks()` in `rbd/destination.go`:

```
for each ChangedBlock:
  if block.Compression == COMPRESSION_LZ4:
    data = lz4.Decompress(
      block.Data, block.UncompressedLength,
    )
  elif block.Compression == COMPRESSION_ZSTD:
    data = zstd.Decompress(
      block.Data, block.UncompressedLength,
    )
  else:
    data = block.Data

  file.WriteAt(data, int64(block.Offset))
```

LZ4 decompression at 10-15 GB/s is not a
bottleneck for any realistic link speed.

### 8.4 Post-Write Verification

After each pwrite completes:

1. Re-read written bytes from `/dev/block`
2. Compute `SHA-256` of read-back data
3. Return `VerifyResult{reqID, matched, hash}`
   to source via `Verify` RPC or inline in
   `SyncResponse`

ACK is sent only after pwrite completes (not on
receive). This bounds receiver memory to
`MaxWindow * ChunkSize` during destination
Ceph degradation.

### 8.5 Modified RBDDataServer

| Current | Pipeline |
|---------|----------|
| Opens `/dev/block` lazily | Same |
| `writeBlocks()`: raw WriteAt | Decompress then WriteAt |
| No hash service | Register `HashServiceServer` |
| No verification | Compute + return post-write hash |
| No reassembly | Fixed-size reassembly buffer |

Register `HashServiceServer` alongside existing
services in `RunDestinationServer()`
(`common/destination.go`). The destination must
accept an optional `HashServiceServer` parameter
or register via a service list.

## 9. Configuration

### 9.1 PipelineConfig Struct

```go
package pipeline

import "time"

// Compressor selects the compression algorithm.
type Compressor int

const (
    CompressorLZ4  Compressor = iota
    CompressorZstd
    CompressorNone
)

// CompressStrategy selects buffer management.
type CompressStrategy int

const (
    CompressInPlace     CompressStrategy = iota
    CompressDoubleBuffer
)

// Config holds all tunable pipeline parameters.
// Zero-value fields are filled by setDefaults().
type Config struct {
    // Semaphore capacities
    MaxRawMemoryBytes        int64
    MaxCompressedMemoryBytes int64
    MaxWindow                int

    // Worker counts
    ReadWorkers     int
    HashWorkers     int
    CompressWorkers int
    HashSendWorkers int
    DataSendWorkers int
    VerifyWorkers   int // 0 = disabled

    // Compression
    Compressor      Compressor
    CompressStrategy CompressStrategy

    // Hash-list batching
    HashBatchMaxCount int
    HashBatchMaxBytes int64
    HashSendMaxBytes  int64

    // Data batching
    DataBatchMaxCount int
    DataBatchMaxBytes int64

    // Channel buffer sizes
    ReadChanBuf     int // default = ReadWorkers
    HashChanBuf     int // default = HashWorkers
    MismatchChanBuf int // default = HashWorkers
    ZeroChanBuf     int // default = ReadWorkers
    CompressChanBuf int // default = CW * 2
    VerifyChanBuf   int // default = VW * 4

    // Runtime
    ChunkSize         int64
    MatchRate         float64
    RTT               time.Duration
    ReadBandwidth     int64   // bytes/sec
    LinkBandwidth     int64   // bytes/sec
    WinPressureThresh float64 // default 0.75
}
```

### 9.2 setDefaults() Contract

`setDefaults()` is called at pipeline start and
fills any zero-value Config field. No magic numbers
exist outside Config. Manual overrides must satisfy
all constraints in Section 9.4 or `setDefaults()`
panics.

### 9.3 Default Values

| Field | Default | Derivation |
|-------|---------|------------|
| ChunkSize | 4 MB | Aligns to default Ceph RBD object size. Misalignment causes read amplification |
| ReadWorkers | 8 | Correct for 1G. For NVMe-backed Ceph raise to 16-32. Cap at N_OSDs*4 |
| HashWorkers | NumCPU/2 | AVX-512 SHA-256 at 8-15 GB/s; 2 cores saturate at 2 GB/s source |
| CompressWorkers | 1 | LZ4 in-place is never CPU-bound. Zstd: override via formula |
| HashSendWorkers | 2 | At RTT=10ms a sender round-trips 100/s; 2 workers sustain 200 hash RPCs/s |
| DataSendWorkers | 4 | Conservative; raise to 8 on 10G or when dest write latency > 5ms |
| VerifyWorkers | 1 | Async metadata only; 1 goroutine faster than any realistic arrival rate |
| MaxWindow | 64 | Safe default for 1G/10ms. Raise to 128-256 for 5G+. Derived: 2*ceil(lambda*RTT) |
| MaxRawMemoryBytes | 256 MB | Worker floor (8*4MB=32MB) plus 8x headroom |
| MaxCompressedMemoryBytes | 0 | LZ4 in-place default; set non-zero only for Zstd |
| HashBatchMaxCount | derived | min(joint_constraint, HashBatchMaxBytes/ChunkSize) with floor=4 |
| HashBatchMaxBytes | derived | HashBatchMaxCount * ChunkSize |
| HashSendMaxBytes | 2 MB | Targets single TCP send buffer fill |
| DataBatchMaxCount | 16 | 16*4MB=64MB per batch |
| DataBatchMaxBytes | 8 MB | Correct for 10G. Reduce to 2 MB for 1G |
| WinPressureThresh | 0.75 | 25% headroom before pipeline stall. Range 0.50-0.90 |
| Compressor | LZ4 | Effectively free (3+ GB/s), 1.5-2x ratio on VM workloads |
| MatchRate | 0.70 | Conservative estimate for incremental RBD sync |
| RTT | 10ms | Mid-range of 5-15ms target envelope |

### 9.4 Hard Limits

| Parameter | Min | Max | Rationale |
|-----------|-----|-----|-----------|
| ChunkSize | 64 KB | 8 MB | Below 64 KB: RBD overhead dominates. Above 8 MB: too large a raw slot fraction |
| MaxWindow | 8 | 4096 | Below 8: stalls on RTT. Above 4096: reassembly buffer unwieldy |
| ReadWorkers | 2 | N_OSDs*4 | Above N_OSDs*4: librbd queue saturates |
| HashWorkers | 1 | NumCPU/2 | Leave half CPU for compress + OS |
| CompressWorkers (LZ4) | 1 | 1 | LZ4 at 3+ GB/s is never the bottleneck |
| HashSendWorkers | 1 | 8 | Above 8: receiver hash processing bottlenecks |
| DataSendWorkers | 1 | 16 | Above 16: dest RBD write concurrency saturates |
| HashBatchMaxCount | 4 | 256 | Below 4: RPC overhead excessive. Must satisfy joint constraint |
| WinPressureThresh | 0.50 | 0.90 | Below 0.50: excessive false flushes. Above 0.90: insufficient headroom |

### 9.5 Parameter Values by Link Class

Reference: 4 MB chunks, RBD source 2 GB/s,
RTT 10 ms, MatchRate 0.70, LZ4 (ratio 2:1).

| Parameter | 1G | 5G | 10G |
|-----------|----|----|-----|
| lambda (chunks/sec) | 208 | 500 | 500 |
| Bottleneck | Link | Read | Read |
| ReadWorkers | 8 | 16 | 32 |
| HashWorkers | 4 | 4 | 4 |
| CompressWorkers (LZ4) | 1 | 1 | 1 |
| HashSendWorkers | 2 | 2 | 4 |
| DataSendWorkers | 2 | 4 | 8 |
| MaxWindow | 32 | 128 | 256 |
| MaxRawMemoryBytes | 64 MB | 128 MB | 256 MB |
| MaxCompressedMemoryBytes | 0 | 0 | 0 |
| HashBatchMaxCount | 6 | 25 | 51 |
| DataBatchMaxCount | 8 | 12 | 16 |
| DataBatchMaxBytes | 2 MB | 4 MB | 8 MB |

### 9.6 Memory Budget Formulas

**Raw memory:**

```
MaxRawMemoryBytes = max(
    ReadWorkers * ChunkSize,          // worker floor
    lambda * ChunkSize * (t_read + RTT_hash)
) * 1.5                               // burst headroom
```

**Compressed memory (Zstd only):**

```
MaxCompressedMemoryBytes = max(
    CompressWorkers * (ChunkSize / r),
    lambda * (1-m) * (ChunkSize / r) * RTT_data
) * 1.5
```

Where `r` = compression ratio, `m` = match rate.

**RTT dominance crossover:**

```
// worker floor > physics when RTT < ReadWorkers / lambda
// At ReadWorkers=8, lambda=500: crossover at RTT=16ms
// For RTT < 16ms: tune ReadWorkers, not MaxRawMemoryBytes
```

## 10. Error Propagation

### 10.1 Held-Resources Struct

```go
type held struct {
    reqID   uint64
    memRawN int64
    memCmpN int64
    hasWin  bool
    hasMem  bool
    hasCmp  bool
}

func (h *held) release(
    memRaw, memCmp *MemSemaphore,
    win *WindowSemaphore,
) {
    if h.hasCmp {
        memCmp.Release(h.memCmpN)
        h.hasCmp = false
    }
    if h.hasMem {
        memRaw.Release(h.memRawN)
        h.hasMem = false
    }
    if h.hasWin {
        win.Release(h.reqID)
        h.hasWin = false
    }
}
```

Every goroutine that may hold semaphore slots
maintains a local `held` struct. All exit paths
including panic recovery call `h.release()`.
Release order is compressed -> raw -> win,
matching reverse acquisition order.

### 10.2 Cancellation

- Root context cancel propagates to all stage
  goroutines (matches current `errgroup` pattern
  in `rbd/source.go`)
- Each stage drains its input channel without
  blocking for new input
- Semaphore slots released via `held.release()`
  before goroutine exits
- Channels drain naturally as upstream stages
  exit; no force-close

**CGO caveat:** librbd reads are blocking CGO
calls; context cancel cannot interrupt them. For
RBD deployments: set a librbd-level timeout
(`rbd_op_thread_timeout`) independent of the Go
context. Without it, a degraded source Ceph
cluster can hold ReadWorkers goroutines alive for
minutes after cancel, delaying shutdown and
holding raw mem slots.

## 11. Correctness Invariants

| Invariant | Consequence of Violation |
|-----------|--------------------------|
| reqIDs are gapless, dense, monotonic from Iterator | base never advances past gap; deadlock after 2*MaxWindow further IDs |
| Every acquired ID released on all exit paths | base pinned permanently; pipeline deadlocks with no error |
| mem_raw acquired before win | ReadWorkers exhaust window while blocked on memory; downstream starves |
| mem_raw capacity > mem_compressed by >= CompressWorkers * ChunkSize | compress workers deadlock acquiring compressed slot while holding raw |
| LZ4 in-place: mem_compressed capacity = 0 | Zstd path accidentally skipped; raw slots never transferred; mem_raw exhausted |
| ACK on pwrite complete, not on receive | receiver memory unbounded during dest Ceph degradation |
| Zero-hash = SHA-256(zero_buf of ChunkSize) at both ends | every sparse chunk treated as mismatch; zero-block optimisation eliminated |
| Snapshot consistency: source must be quiesced or detached snapshot | chunks from different points in time; receiver writes torn image |
| HashBatchMaxCount satisfies joint constraint | partial batches exhaust window at low throughput; pipeline stalls |
| mem_raw.Capacity > mem_compressed.Capacity >= 0 | setDefaults() panics on startup if violated |

## 12. File Map

### 12.1 New Files

| File | Package | Purpose |
|------|---------|---------|
| `internal/proto/api/v1/hash.proto` | api.v1 | HashService proto definition |
| `internal/proto/api/v1/hash.pb.go` | (generated) | Protobuf stubs |
| `internal/proto/api/v1/hash_grpc.pb.go` | (generated) | gRPC stubs |
| `internal/worker/rbd/pipeline/config.go` | pipeline | Config, setDefaults() |
| `internal/worker/rbd/pipeline/semaphore.go` | pipeline | MemSemaphore, WindowSemaphore |
| `internal/worker/rbd/pipeline/chunk.go` | pipeline | Chunk type definitions |
| `internal/worker/rbd/pipeline/stage_read.go` | pipeline | StageRead workers |
| `internal/worker/rbd/pipeline/stage_hash.go` | pipeline | StageHash workers |
| `internal/worker/rbd/pipeline/stage_send_hash.go` | pipeline | StageSendHash batcher + senders |
| `internal/worker/rbd/pipeline/stage_compress.go` | pipeline | StageCompress workers |
| `internal/worker/rbd/pipeline/stage_send_data.go` | pipeline | StageSendData workers |
| `internal/worker/rbd/pipeline/stage_verify.go` | pipeline | StageVerify worker |
| `internal/worker/rbd/pipeline/pipeline.go` | pipeline | Orchestrator, wires stages |
| `internal/worker/rbd/pipeline/held.go` | pipeline | held struct, resource cleanup |
| `internal/worker/rbd/hash_server.go` | rbd | HashServiceServer implementation |

### 12.2 Modified Files

| File | Before | After |
|------|--------|-------|
| `rbd/source.go` `Sync()` | 2-goroutine errgroup: produceBlocks + StreamBlocks | Calls `pipeline.New(cfg).Run(ctx, iter, device, conn)` |
| `rbd/destination.go` `writeBlocks()` | Raw WriteAt for data/zero blocks | Decompress (LZ4/Zstd) then WriteAt; post-write hash |
| `rbd/destination.go` `RBDDataServer` | Only DataServiceServer | Also implements or hosts HashServiceServer |
| `proto/api/v1/data.proto` `ChangedBlock` | 4 fields | 7 fields (+request_id, compression, uncompressed_length) |
| `proto/api/v1/data.proto` `DataService` | Sync + Delete | Sync + Delete + Verify |
| `common/destination.go` `RunDestinationServer()` | Registers Data + Version + Done | Also registers HashService |
| `common/constants.go` | Payload/timeout constants | Unchanged; pipeline constants live in pipeline/config.go |

### 12.3 Unchanged (Still Used by CephFS)

- `common/streamer.go` -- `StreamBlocks()`,
  `ReadBlockEntry()` remain for CephFS path
- `common/client.go` -- `ConnectToDestination()`,
  `SignalDone()` still used by pipeline
- `common/util.go` -- `IsAllZero()` still used
  by StageRead
- `ceph/rbd.go` -- `RBDBlockDiffIterator` stays
  as-is; pipeline StageRead consumes it

## 13. Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Semaphore deadlock from incorrect acquisition order | Pipeline hangs silently | Enforce order via held struct; unit test with concurrent stages |
| CGO librbd reads block context cancel | Shutdown delays up to minutes | Document rbd_op_thread_timeout; add deadline to ReadAt wrapper |
| Hash comparison doubles destination I/O | Slower for low match rates | Skip hash stage when MatchRate < threshold (configurable) |
| Proto field additions break old destinations | Incompatible versions | VersionService handshake; fallback to current 2-goroutine model |
| Memory budget formula underestimates for bursty workloads | OOM in mover pod | 1.5x headroom factor; setDefaults() validates at startup |
| Window pressure threshold too aggressive | Excessive partial hash batches | Default 0.75 with tuning range 0.50-0.90 |

## 14. Success Criteria

- `make docker-build-mover` succeeds with new
  pipeline package
- Pipeline saturates 1G/5G/10G links in
  benchmarks with bounded memory
- `make test` passes with unit tests for
  semaphores, stages, config validation
- Zero-block short-circuit: no hash/compress/send
  for zero chunks in test
- Hash dedup: matched chunks release resources
  at StageSendHash in test
- Compression: verify LZ4 in-place and Zstd
  double-buffer paths
- Verify stage: detect injected bitrot in
  integration test
- Backward compatible: old destinations work
  via version handshake fallback
- All correctness invariants from Section 11
  verified in tests
