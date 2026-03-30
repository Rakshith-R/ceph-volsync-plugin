# rbd

RBD block device operations and snapshot diff iteration.

## Overview

This package provides utilities for working with RBD (RADOS Block Device) images, including connection management, image access, and block-level snapshot difference iteration.

## Key Functions

### Connection and Image Management

- **NewClusterConnection(monitors)**: Creates a new cluster connection
- **NewImage(cc, imageSpec)**: Opens an RBD image using spec format `pool/[namespace/]imageName`
- **NewImageBySpec(cc, spec)**: Opens an RBD image from a go-ceph ImageSpec
- **PoolNameByID(cc, poolID)**: Resolves pool ID to pool name
- **RBDImageSpec(pool, radosNS, imageName)**: Builds image spec string
- **SnapshotIDByName(image, snapName)**: Finds snapshot ID by name

## Key Types

### ChangeBlock

Represents a changed block region with offset and length.

### RBDBlockDiffIterator

Efficiently iterates over changed blocks between two RBD snapshots using go-ceph's `DiffIterateByID` API.

**Usage**:
```go
iter, err := rbd.NewRBDBlockDiffIterator(monitors, poolID, namespace, imageName,
                                          fromSnapID, targetSnapID, volSize)
if err != nil { return err }
defer iter.Close()

for {
    block, ok := iter.Next()
    if !ok { break }
    // Process block.Offset and block.Len
}
```

**Methods**:
- `Next()`: Returns next changed block or nil when done
- `Close()`: Drains iterator, waits for diff operation to complete, and releases resources

## Implementation Details

- Uses buffered channel (64 blocks) for async iteration
- Runs diff operation in background goroutine
- Proper cleanup even on errors (prevents goroutine/resource leaks)
- Supports whole-object mode and parent snapshot inclusion
