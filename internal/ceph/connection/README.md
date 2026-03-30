# connection

Ceph cluster connection management with pooling.

## Overview

This package provides connection pooling and lifecycle management for Ceph cluster connections. It minimizes connection overhead by reusing connections and implements garbage collection for idle connections.

## Key Types

### ClusterConnection

Represents a single connection to a Ceph cluster with convenience methods for common operations.

**Methods**:
- `Connect(monitors)`: Establishes connection using credentials from mounted CSI secret
- `Destroy()`: Returns connection to the pool
- `GetIoctx(pool)`: Opens an IOContext for RADOS operations
- `GetPoolByID(poolID)`: Resolves pool ID to pool name
- `GetFSAdmin()`: Returns FSAdmin for CephFS administration
- `CreateMountFromRados()`: Creates CephFS mount from existing RADOS connection

### ConnPool

Connection pool that manages multiple RADOS connections with automatic garbage collection.

**Features**:
- Reference counting to track active users of each connection
- Periodic cleanup of idle connections (default: 15min interval, 10min expiry)
- Thread-safe access with read-write locks
- Unique key generation based on monitors, user, and credentials

**Methods**:
- `Get(monitors, user, keyfile)`: Retrieves or creates a connection
- `Put(conn)`: Returns a connection to the pool

## Implementation Details

- Reads Ceph credentials from mounted secret at `/csi-secret/` (userID and key)
- Creates `/etc/ceph/ceph.conf` automatically via cephconf package
- Uses SHA-256 hashing to generate unique connection keys
- Runs background timer for periodic garbage collection
