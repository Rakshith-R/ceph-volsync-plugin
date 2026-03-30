# ceph

Ceph storage integration library.

## Overview

This package provides Go abstractions for interacting with Ceph storage clusters, including connection management, CephFS operations, RBD device management, and CSI volume ID handling.

## Package Structure

### [cephconf/](cephconf/)
Ceph configuration file generation - creates `/etc/ceph/ceph.conf` and keyring files.

### [cephfs/](cephfs/)
CephFS mount and snapshot diff operations - provides iterators for directory-level and block-level snapshot comparisons.

### [connection/](connection/)
Ceph cluster connection pooling and lifecycle management - minimizes connection overhead with automatic garbage collection.

### [errors/](errors/)
Ceph-specific error types - sentinel errors for common failure conditions.

### [rbd/](rbd/)
RBD block device operations - image access, snapshot diff iteration, and pool management.

### [volid/](volid/)
CSI volume ID encoding/decoding - reversible transformation between volume metadata and CSI-compliant identifier strings.

## Dependencies

This package wraps [go-ceph](https://github.com/ceph/go-ceph) with plugin-specific abstractions:
- `github.com/ceph/go-ceph/rados` - RADOS operations
- `github.com/ceph/go-ceph/rbd` - RBD operations
- `github.com/ceph/go-ceph/cephfs` - CephFS operations
- `github.com/ceph/go-ceph/cephfs/admin` - CephFS administration

## Common Patterns

### Establishing Connections

All operations start with a cluster connection:

```go
cc := &connection.ClusterConnection{}
err := cc.Connect(monitors)
defer cc.Destroy()
```

### Snapshot Diff Operations

CephFS and RBD both provide iterator-based snapshot diff APIs for efficient change detection.

### Resource Management

All types follow the pattern: create → use → cleanup (`Destroy()` or `Close()`)
