# rbd

RBD worker implementation for block device source and destination data transfer.

## Overview

This package implements RBD-specific workers that perform efficient snapshot-based block device replication between source and destination volumes. It operates on raw block devices and uses RBD's snapshot diff capabilities for optimized data transfer.

## Key Components

### SourceWorker

Main entry point for the source side that orchestrates RBD block device synchronization.

**Features**:
- Decodes CSI volume/snapshot handles to extract RBD metadata
- Resolves parent image and snapshot IDs from snapshot metadata
- Creates RBD block diff iterator using go-ceph
- Runs concurrent pipeline for changed blocks
- Supports full diff (no snapshots) and incremental diff (with base snapshot)
- Waits for all blocks to be ACKed before sending commit
- Operates on `/data/rbd-pv` device path

**Implementation**: Embeds `common.BaseSourceWorker` and implements `common.Syncer` interface.

### DestinationWorker

Destination side worker that receives and applies block changes via gRPC.

**Implementation**: Embeds `common.BaseDestinationWorker` and runs servers for Write, Delete, CompareHashes, and Commit operations on the block device.

### RBDDataServer

Destination-side handler for `Write` and `Delete` RPCs on block devices.

**Write**:
- Bidi-streaming RPC that receives block writes
- Opens block device lazily on first write (O_RDWR)
- Decompresses LZ4-compressed blocks
- Writes directly to device using pwrite() at specified offsets
- ACKs request IDs after each batch
- Syncs and closes device when stream completes

**Delete**: No-op for block devices (not applicable).

### HashServer

Implements hash comparison for detecting identical blocks.

**Protocol**:
- Opens device read-only
- For each block hash request, reads device at given offset
- Computes SHA-256 and compares with source hash
- Returns list of mismatched request IDs

**Optimization**: Allows pipeline to skip sending blocks that already match at destination.

### RBDCommitServer

Handles commit operations on the destination block device.

**Protocol**:
- Receives batched commit requests
- Opens device with O_SYNC for synchronous writes
- Syncs device to ensure durability
- ACKs each batch

### rbdIterAdapter

Adapts `cephrbd.RBDBlockDiffIterator` to `pipeline.BlockIterator` interface.

**Functionality**:
- Wraps RBD-specific ChangeBlock with pipeline ChangeBlock
- Assigns sequential request IDs
- Sets FilePath to constant `/data/rbd-pv`
- Includes total volume size for metadata

### fileDataReader

Adapts `os.File` to `pipeline.DataReader` interface for reading from block device.

**Functionality**:
- Implements ReadAt() using file.ReadAt()
- CloseFile() is no-op (device handle managed separately)

## Data Flow

### Source Side

1. **Configuration Resolution**:
   - Parse VOLUME_HANDLE, BASE_SNAPSHOT_HANDLE, TARGET_SNAPSHOT_HANDLE
   - Resolve Ceph cluster monitors and RADOS namespace
   - Connect to Ceph cluster

2. **Snapshot Resolution**:
   - If both snapshot handles missing: full diff from volume
   - If only target snapshot: full diff from target snapshot
   - If both snapshots: incremental diff between base and target

3. **Parent Image Discovery**:
   - Open target snapshot image
   - Call GetParent() to find parent image spec
   - Resolve snapshot IDs on parent image

4. **Block Diff Iteration**:
   - Create RBDBlockDiffIterator with resolved snapshot IDs
   - Open source block device (/data/rbd-pv)
   - Run pipeline: Iterator → Read → Hash → SendHash → Compress → SendData

5. **Commit**:
   - Wait for all blocks to be ACKed (check sliding window)
   - Send single commit entry for device path
   - Signal Done to destination

### Destination Side

1. Start gRPC sync server with Write/Hash/Commit handlers
2. **Write handler**: Receive blocks, decompress, write to device, ACK
3. **Hash handler**: Read device blocks, compare SHA-256, return mismatches
4. **Commit handler**: Sync device to ensure durability, ACK

## Configuration

### Environment Variables (Source)

- `VOLUME_HANDLE`: CSI volume ID (required)
- `BASE_SNAPSHOT_HANDLE`: CSI snapshot ID of base snapshot (optional)
- `TARGET_SNAPSHOT_HANDLE`: CSI snapshot ID of target snapshot (optional)

### Constants

- `DevicePath`: `/data/rbd-pv` (block device mount point)

## Diff Modes

### Full Diff (No Snapshots)

When snapshot handles are not provided:
- Parent image = volume image (`csi-vol-{uuid}`)
- fromSnapID = 0, targetSnapID = 0
- Transfers all non-zero blocks

### Full Diff (Target Snapshot Only)

When only TARGET_SNAPSHOT_HANDLE is provided:
- Opens target snapshot, gets parent image
- fromSnapID = 0, targetSnapID = resolved from parent info
- Transfers all blocks changed from beginning to target snapshot

### Incremental Diff (Both Snapshots)

When both BASE_SNAPSHOT_HANDLE and TARGET_SNAPSHOT_HANDLE are provided:
- Opens target snapshot, gets parent image
- Opens base snapshot, gets parent info
- fromSnapID = base snapshot ID, targetSnapID = target snapshot ID
- Transfers only blocks changed between base and target

## Integration

### Pipeline Package

- Provides `pipeline.BlockIterator` implementation (rbdIterAdapter)
- Provides `pipeline.DataReader` implementation (fileDataReader)
- Uses `pipeline.New()` to run 6-stage concurrent pipeline

### Common Package

- Embeds `BaseSourceWorker` and `BaseDestinationWorker`
- Uses `NewSyncServer()` to create gRPC server
- Implements `Syncer` interface for custom sync logic
- Uses `SignalDone()` to notify completion

### Ceph Package

- Uses `cephrbd.NewRBDBlockDiffIterator` for snapshot diff
- Uses `cephrbd.NewClusterConnection` for cluster access
- Uses `cephrbd.NewImage` to open RBD images
- Uses `volid.CSIIdentifier` for decoding volume/snapshot handles
- Uses `config` package for cluster configuration

### Tunnel Package

- Uses tunneled connections for gRPC communication (managed by common package)

## Block Device Operations

All device operations use standard Go file I/O:
- **Open**: `os.OpenFile(devicePath, O_RDWR, 0)`
- **Read**: `file.ReadAt(buffer, offset)`
- **Write**: `file.WriteAt(buffer, offset)` (via pwrite in writeBlocks)
- **Sync**: `file.Sync()` for durability
- **Close**: `file.Close()` to release device handle

## Testing

Unit tests cover:
- Hash server with various scenarios (match, mismatch)
- Destination write operations with compression
