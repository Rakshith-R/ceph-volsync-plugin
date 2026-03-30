# cephfs

CephFS worker implementation for source and destination data transfer.

## Overview

This package implements CephFS-specific workers that perform efficient snapshot-based replication between source and destination volumes. It integrates with the pipeline, common, and tunnel packages to provide block-level differential sync.

## Key Components

### SourceWorker

Main entry point for the source side that orchestrates CephFS snapshot diff synchronization.

**Features**:
- Decodes CSI volume/snapshot handles to extract CephFS metadata
- Creates snapshot differ using base and target snapshots
- Categorizes changes into large files (block diff), small files (rsync), deletions, and directories
- Runs concurrent pipelines for each category
- Falls back to rsync when snapshot handles are not provided

**Implementation**: Embeds `common.BaseSourceWorker` and implements `common.Syncer` interface.

### DestinationWorker

Destination side worker that receives and applies changes via gRPC.

**Implementation**: Embeds `common.BaseDestinationWorker` and runs servers for Write, Delete, CompareHashes, and Commit operations using a shared FileCache.

### CephFSBlockIterator

Implements `pipeline.BlockIterator` to flatten block diffs across multiple large changed files.

**Modes**:
- Slice-fed: Iterate over a static list of file paths
- Channel-fed: Consume files from a channel as they're discovered (used with concurrent directory walking)

**Features**:
- Per-file block diff iteration using go-ceph's BlockDiffIterator (via adapter)
- Emits file boundaries to coordinate commit operations
- Context-aware for graceful cancellation
- Configurable via functional options (size function, boundary channel, file channel)

### CephFSReader

Implements `pipeline.DataReader` for reading CephFS files from the mounted snapshot.

**Features**:
- Shared FileCache for concurrent file access with reference counting
- ReadAt() for random access to file blocks
- CloseFile() releases handle after commits

**Usage**: Created by source worker and passed to pipeline stages.

### CommitDrainer

Coordinates file commits by monitoring pipeline progress and ensuring blocks are acknowledged before committing.

**Strategy**:
- Receives file boundaries from block iterator
- Waits for blocks to be ACKed by destination (checks sliding window)
- Batches commits to reduce RPC overhead
- Sends commit batches to destination
- Notifies metadata rsync goroutine about committed files
- Final flush waits for all pending files to be ACKed

**Prevents**: Committing files before their blocks are fully written to destination.

### DataServer

Destination-side handler for `Write` and `Delete` RPCs.

**Write**: Bidi-streaming RPC that receives blocks for one or more files, writes them to disk via FileCache, and ACKs request IDs.

**Delete**: Bidi-streaming RPC that receives batches of paths to delete and ACKs each batch.

### CephFSHashServer

Implements hash comparison for detecting identical blocks between source and destination.

**Protocol**: For each block, reads destination file at given offset, computes SHA-256, compares with source hash, and returns mismatched request IDs.

**Optimization**: Allows pipeline to skip sending blocks that already match.

### CephFSCommitServer

Handles commit operations on the destination.

**Protocol**: Receives batches of files to commit, fsyncs and releases each file handle via FileCache, ACKs each batch.

### FileCache

Thread-safe cache for CephFS file handles with reference counting.

**Modes**:
- Read cache: Opens files read-only with O_RDONLY
- Write cache: Opens/creates files for writing with O_RDWR|O_CREATE

**Features**:
- Lazy open on first Acquire()
- Reference counting for concurrent access
- Automatic close when ref count reaches zero
- SyncAndRelease() for commit operations

## Data Flow

### Source Side

1. Parse snapshot handles and create CephFS SnapshotDiffer
2. Walk directory tree using SnapDiffIterator
3. Categorize entries:
   - Large files (>64KB) → block diff pipeline
   - Small files (≤64KB) → rsync
   - Deletions → Delete RPC
   - Directories → recursive walk
4. For large files:
   - Block iterator generates changed blocks
   - Pipeline: Read → Hash → SendHash → Compress → SendData
   - Commit drainer waits for ACKs and sends batched commits
   - Metadata rsync updates timestamps/permissions for committed files
5. Signal Done when all operations complete

### Destination Side

1. Start gRPC sync server with Write/Delete/Hash/Commit handlers
2. Write handler: Receive blocks, write to FileCache, ACK request IDs
3. Delete handler: Remove paths, ACK batches
4. Hash handler: Read local blocks, compare SHA-256, return mismatches
5. Commit handler: Fsync files, release handles, ACK batches

## Configuration

### Environment Variables (Source)

- `BASE_SNAPSHOT_HANDLE`: CSI snapshot ID of base snapshot
- `TARGET_SNAPSHOT_HANDLE`: CSI snapshot ID of target snapshot
- `VOLUME_HANDLE`: CSI volume ID
- `RSYNC_DAEMON_PORT`: Destination rsync daemon port (default: 8873)

### Constants

- `smallFileMaxSize`: 64KB (files ≤ this size use rsync instead of block diff)
- `deleteBatchSize`: 2000 (paths per delete RPC batch)
- `smallBatchSize`: 500 (paths per small file rsync batch)
- `maxRetries`: 5 (rsync retry attempts)

## Integration

### Pipeline Package

- Provides `pipeline.BlockIterator` implementation (CephFSBlockIterator)
- Provides `pipeline.DataReader` implementation (CephFSReader)
- Uses `pipeline.New()` to run 6-stage concurrent pipeline

### Common Package

- Embeds `BaseSourceWorker` and `BaseDestinationWorker`
- Uses `NewSyncServer()` to create gRPC server
- Implements `Syncer` interface for custom sync logic

### Tunnel Package

- Rsync operations use tunneled connections established by common package

### Ceph Package

- Uses `cephfs.SnapshotDiffer` for snapshot operations
- Uses `volid.CSIIdentifier` for decoding volume/snapshot handles
- Uses `config` package for cluster configuration

## Testing

Unit tests cover:
- Block iterator with mock file diff iterators
- CephFS reader with temporary files
- Hash server with various scenarios (match, mismatch, missing file)
