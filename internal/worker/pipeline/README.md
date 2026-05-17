# Pipeline Package

High-performance concurrent data transfer pipeline for block-level replication.

## Overview

The pipeline package implements a multi-stage concurrent architecture for efficient block data transfer from source to destination. It orchestrates parallel workers across 3 stages with semaphore-based flow control to maximize throughput while controlling memory usage and network pressure.

## Architecture

### Pipeline Flow

```
BlockIterator → Feeder → Read → SendData → gRPC Stream
                  ↓        ↓        ↓
              ReqID     Buffers  Batched Send
```

### Stages

1. **Feeder**: Assigns unique request IDs to blocks from the iterator and emits `Chunk` metadata
2. **Read**: Parallel workers (`ReadWorkers`) read raw data from disk/device via `DataReader.ReadAt()`
   - Detects all-zero blocks early, releases memory immediately, and marks them with `IsZero` flag
   - Acquires memory from `MemSemaphore` and window slots from `WindowSemaphore`
3. **SendData**: Parallel workers (`DataSendWorkers`) batch and send chunks over gRPC
   - Batches up to `DataBatchMaxCount` chunks or `DataBatchMaxBytes` bytes per RPC

## Key Types

### Pipeline

```go
type Pipeline struct {
    cfg Config
}
```

Main orchestrator. Create with `New(cfg Config)` and run with `Run()`.

**Methods:**
- `Run(ctx, iter, reader, newStream, win) error`: Executes the full pipeline until all blocks are processed or context is canceled

### Config

Tunable parameters controlling pipeline behavior:

```go
type Config struct {
    MaxRawMemoryBytes int64  // Total memory pool for raw data (default: 1GB)
    MaxWindow         int    // Max in-flight request ID spread (default: 1024)

    ReadWorkers       int    // Parallel read workers (default: 8)
    DataSendWorkers   int    // Parallel data stream workers (default: 8)

    DataBatchMaxCount int    // Max data chunks per batch (default: 16)
    DataBatchMaxBytes int64  // Max bytes per data batch (default: 16MB)

    ChunkSize         int64  // Block size for reads (default: 16MB, range: 64KB-16MB)
    ReadChanBuf       int    // Channel buffer size (auto-tuned)
}
```

Call `cfg.SetDefaults()` to auto-populate zero fields with sensible defaults.

### Chunk Types

Data flows through typed channel stages:

- **`Chunk`**: Metadata only (reqID, path, offset, length)
- **`ReadChunk`**: Adds raw `Data []byte`, `IsZero` flag, and resource tracking. Zero blocks have `Data: nil` and `IsZero: true`

Each chunk carries a `held` struct tracking acquired semaphore resources.

### Semaphores

#### MemSemaphore

Weighted semaphore for raw memory allocation with FIFO wake discipline.

```go
func NewMemSemaphore(capacity int64) *MemSemaphore
func (s *MemSemaphore) Acquire(ctx, n int64) error
func (s *MemSemaphore) Release(n int64)
```

Prevents unbounded memory growth by blocking readers when total allocated bytes reach `MaxRawMemoryBytes`.

#### WindowSemaphore

Bounds the spread of in-flight request IDs to control network and commit pressure.

```go
func NewWindowSemaphore(maxWindow int) *WindowSemaphore
func (w *WindowSemaphore) Acquire(ctx, reqID uint64) error
func (w *WindowSemaphore) Release(reqID uint64)
```

**Key features:**
- Maintains a sliding window of size `2*MaxWindow`
- Releases advance the `base` pointer, sliding the window forward

### Interfaces

#### BlockIterator

```go
type BlockIterator interface {
    Next() (*ChangeBlock, bool)
    Close() error
}
```

Provides changed blocks from snapshot diff. Typically wraps RBD diff iteration.

#### DataReader

```go
type DataReader interface {
    ReadAt(filePath string, offset, length int64) ([]byte, error)
    CloseFile(filePath string) error
}
```

Abstracts block-level reads. Must be **safe for concurrent calls** from multiple `ReadWorkers`.

#### StreamFactory

```go
type StreamFactory func(context.Context) (
    grpc.BidiStreamingClient[WriteRequest, WriteResponse], error)
```

Factory function opening new gRPC streams per worker.

## Concurrency Model

### Worker Parallelism

Each stage spawns configurable worker goroutines that pull from input channels and push to output channels:

- **Read**: `ReadWorkers` goroutines calling `DataReader.ReadAt()` in parallel
- **SendData**: `DataSendWorkers` gRPC streams for data transfer

### Flow Control

**Memory pressure:** `MemSemaphore` blocks readers when raw buffer pool is exhausted. Resources are released in FIFO order.

**Window pressure:** `WindowSemaphore` blocks feeder when too many requests are in-flight. Prevents overwhelming the destination with uncommitted writes.

**Backpressure:** Buffered channels propagate backpressure upstream when downstream stages slow down.

### Resource Management

The `held` struct tracks semaphore acquisitions per chunk:

```go
type held struct {
    reqID   uint64
    memRawN int64  // bytes held in MemSemaphore
    hasWin  bool   // owns window slot
    hasMem  bool   // owns memory
}
```

**Release discipline:** Resources are freed in reverse acquisition order (memory -> window) via `held.release()`. All exit paths (success, error, cancellation) must call `release()`.

**Split release:** After `SendData` transmits a chunk, it calls `releaseMemOnly()` to free memory while retaining the window slot until the destination ACKs the write.

## Error Handling

Uses `golang.org/x/sync/errgroup` for structured concurrency:

- Any stage error cancels the shared context, stopping all workers
- All goroutines are waited on via `errgroup.Wait()`
- Channels are closed in reverse dependency order when stages exit
- Resources held by in-flight chunks are released on context cancellation

## Configuration Guidelines

### Memory Tuning

- **`MaxRawMemoryBytes`**: Total memory budget for uncompressed data. Set to avoid OOM while maximizing parallelism.
- **`ChunkSize`**: Larger chunks improve throughput but increase latency and memory per chunk.
- Ensure `ChunkSize << MaxRawMemoryBytes` to allow concurrent reads.

### Worker Tuning

- **`ReadWorkers`**: Scale with I/O parallelism (8-16 for NVMe, 4-8 for spinning disks)
- **`DataSendWorkers`**: Scale with network bandwidth and latency (4-8 for WAN)

### Window Tuning

- **`MaxWindow`**: Controls commit granularity and retransmit cost. Higher values improve throughput but increase recovery time on failure.

## Usage Example

```go
cfg := pipeline.Config{
    MaxRawMemoryBytes: 512 * 1024 * 1024, // 512MB
    MaxWindow:         128,
    ReadWorkers:       16,
    DataSendWorkers:   8,
}

p := pipeline.New(cfg)

win := pipeline.NewWindowSemaphore(cfg.MaxWindow)

err := p.Run(ctx, blockIter, dataReader, newDataStream, win)
if err != nil {
    return fmt.Errorf("pipeline failed: %w", err)
}
```

## Testing

The package includes comprehensive unit tests:

- `config_test.go`: Config validation and defaults
- `semaphore_test.go`: MemSemaphore and WindowSemaphore correctness
- `stage_*_test.go`: Individual stage behavior
- `pipeline_test.go`: End-to-end integration tests
- `held_test.go`: Resource tracking

Run tests with:
```bash
go test ./internal/worker/pipeline
```
