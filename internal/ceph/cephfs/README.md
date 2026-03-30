# cephfs

CephFS mount and snapshot diff operations.

## Overview

This package provides high-level abstractions for working with CephFS snapshots and computing differences between them. It supports both directory-level and block-level snapshot comparisons.

## Key Types

### SnapshotDiffer

Main entry point that manages the CephFS connection, mount, and snapshot contexts. Provides factory methods for creating iterators.

**Usage**:
```go
differ, err := cephfs.New(monitors, fsID, subVolumeGroup, subVolumeName, "snap1", "snap2")
if err != nil { return err }
defer differ.Destroy()
```

### SnapDiffIterator

Iterates over directory-level differences between two snapshots. Does not recurse; caller is responsible for traversing subdirectories.

**Methods**:
- `Read()`: Returns next directory entry that changed between snapshots
- `Close()`: Releases snapshot diff resources

### BlockDiffIterator

Provides block-level iteration over changed regions within a single file between snapshots.

**Methods**:
- `Read()`: Returns next changed block with offset and length
- `Close()`: Releases block diff resources

## Implementation Details

- Mounts CephFS at the subvolume path using `MountWithRoot()` to support restricted Ceph users
- Uses go-ceph's `SnapDiffInfo` and `FileBlockDiffInfo` APIs
- Properly handles resource cleanup via `Destroy()` and `Close()` methods
