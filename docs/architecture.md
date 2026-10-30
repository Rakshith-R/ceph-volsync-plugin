# Architecture

## System Overview

The ceph-volsync-plugin consists of two binaries:

- **Manager** (`cmd/manager/main.go`): A Kubernetes operator that watches `ReplicationSource` and `ReplicationDestination` custom resources and orchestrates data replication by creating K8s Jobs.
- **Mover** (`cmd/mover/main.go`): A data transfer binary that runs inside those K8s Jobs and performs the actual volume synchronization using Ceph-native snapshot diff APIs.

## Component Architecture

```
+--------------------------------------------------------+
|                   Manager (Operator)                   |
|  +-----------------+  +--------------------------+     |
|  | ReplicationSrc  |  | ReplicationDest          |     |
|  |   Controller    |  |   Controller             |     |
|  +--------+--------+  +-------------+------------+     |
|           |                         |                  |
|  +--------v-------------------------v---------------+  |
|  |            Mover Builder (ceph)                  |  |
|  | Detects CSI provider -> constructs Mover         |  |
|  +--------+-------------------------+---------------+  |
|           |                         |                  |
|   K8s Job (source)          K8s Job (destination)      |
+-----------+-------------------------+------------------+
            |                         |
+-----------v-------------------------v------------------+
|                     Mover Pods                         |
|  +----------------+         +--------------------+     |
|  | Source Worker  |-------->| Destination Worker |     |
|  | (CephFS/RBD)   |  mTLS   | (CephFS/RBD)       |     |
|  +-------+--------+         +---------+----------+     |
|          |                            |                |
|  +-------v--------+         +---------v----------+     |
|  | Pipeline       |         | gRPC Server        |     |
|  | Read->SendData |         | Write / Delete /   |     |
|  | (concurrent)   |         | Commit / Done      |     |
|  +----------------+         +--------------------+     |
+--------------------------------------------------------+
```

## Controller Layer

Both controllers (`internal/controller/`) implement VolSync's `statemachine.ReplicationMachine` interface, which defines a three-phase reconcile loop:

1. **Ensure prerequisites** — verify secrets, services, service accounts, and PVCs exist.
2. **Synchronize** — create or monitor the mover Job; wait for completion.
3. **Cleanup** — remove temporary resources after a successful sync.

On each reconcile the controller:
- Looks up the correct mover from the catalog by matching the CR's `spec.external.provider` field.
- Delegates all state transitions to VolSync's state machine.
- Watches Jobs, Secrets, Services, PVCs, VolumeSnapshots, and ConfigMaps as secondary resources so changes trigger re-queues.

## Mover Builder

`internal/mover/ceph/builder.go` implements the `mover.Builder` interface registered with VolSync's mover catalog.

**CSI provider detection** (from `spec.external.provider`):

| Provider suffix | Mover type |
|---|---|
| `cephfs.csi.ceph.com` | CephFS |
| `nfs.csi.ceph.com` | CephFS |
| `rbd.csi.ceph.com` | RBD |
| `nvmeof.csi.ceph.com` | RBD |

`FromSource()` and `FromDestination()` construct a `Mover` struct that:
- Reads CR parameters (`storageClassName`, `copyMethod`, `keySecret`, etc.).
- Creates a `VolumeHandler` for snapshot/PVC lifecycle.
- Passes the detected mover type to the K8s Job via `MOVER_TYPE` env var.

## Data Flow — CephFS

```
Source PVC
    |
    v VolumeSnapshot (target)
SnapshotDiffer (base vs target)
    |
    +-- Non-empty regular files --> Block pipeline (gRPC Write)
    +-- Special/zero-length files --> rsync (symlinks, sockets, empty files)
    +-- Deletions     --> gRPC Delete
    +-- Dir metadata  --> convergence rsync
                                |
                         Destination Worker
                                |
                           FileCache
                                |
                        atomic Commit -> dest PVC
```

Steps:
1. Source worker decodes CSI volume and snapshot handles to extract CephFS cluster, pool, and path metadata.
2. `SnapshotDiffer` computes the diff between base and target snapshots using `cephfs.SnapDiffIterator`.
3. Changed entries are categorized: non-empty regular files go to the block pipeline, special and zero-length files go to rsync, deletions go to gRPC Delete.
4. The block pipeline runs Read -> SendData concurrently, bounded by a semaphore.
5. Destination applies writes to a `FileCache` and atomically commits on `Done`.
6. When no snapshot handles are provided, falls back to full rsync.

## Data Flow — RBD

```
Source Block Device (/dev/block)
    |
    v RBD diff iterator (go-ceph)
Changed blocks (offset, length, data)
    |
    +-- Non-zero blocks --> gRPC Write (with data)
    +-- Zero blocks     --> gRPC Write (is_zero=true, no payload)
                                |
                         Destination Worker
                                |
                    Write to /dev/block at offset
                                |
                        gRPC Done -> complete
```

Steps:
1. Source worker decodes CSI handles to resolve the RBD pool, image name, and snapshot IDs.
2. Creates an RBD diff iterator between base and target snapshots (or full image for first sync).
3. Runs a concurrent block pipeline — blocks are read and sent via gRPC Write.
4. Zero blocks are detected at the Read stage; only the `is_zero=true` flag is sent (no payload).
5. Whole-object mode aggregates adjacent changed extents for efficient sequential I/O.
6. Source waits for all block ACKs before sending `Done`.

## Pipeline Architecture

`internal/worker/pipeline/` implements the concurrent data transfer pipeline used by both CephFS and RBD workers.

```
 +----------+    chunk channel    +--------------+
 |  Read    | ------------------> |  SendData    |
 |  stage   |                     |   stage      |
 | N workers|                     | (N workers)  |
 +----------+                     +--------------+
```

- Two stages: **Read** (read from Ceph) and **SendData** (send via gRPC).
- Each stage runs N concurrent workers bounded by a semaphore.
- Chunks carry an `IsZero` flag; zero chunks skip payload serialization in SendData.
- `errgroup` from `golang.org/x/sync` manages goroutine lifecycle and error propagation.
- `PipelineConfig` controls worker counts and chunk size.

## Networking

The plugin uses a two-phase connection model: stunnel for the control channel, then direct TLS for data transfer.

```
Source Pod                            Destination Pod
+-------------------+                +------------------------------+
| Source Worker     |                | Destination Worker           |
|     |             |                |       |                      |
| 1. stunnel client-+-PSK-TLS:8000-->| stunnel server               |
|     |             | (handshake +   |   | -> gRPC server :8080     |
|     |             | ExchangeCerts) |       |                      |
|     |             |                |       |                      |
| 2. gRPC client ---+--mTLS:8081---->| direct TLS listener          |
|   (Write/Delete/  | (data xfer)    | (Write/Delete/Commit/Done)   |
|    Commit/Done)   |                |       |                      |
|     |             |                | (CephFS only)                |
| rsync client -----+--TLS:8873----->| rsync stunnel->daemon:8874   |
+-------------------+                +------------------------------+
```

**Phase 1 — Control channel (stunnel, port 8000):**
- PSK-authenticated TLS tunnel established between source and destination.
- Source calls `ExchangeCerts` RPC: sends its ephemeral TLS certificate, receives the destination's certificate and direct TLS port.
- This channel is NOT used for data transfer.

**Phase 2 — Data channel (direct TLS, port 8081):**
- Source opens a direct gRPC connection to the destination's direct TLS listener.
- mTLS with ephemeral certificates pinned by SHA-256 fingerprint.
- All data RPCs (`Write`, `Delete`, `Commit`, `Done`) flow over this connection.

**Rsync tunnel (CephFS only):**
- CephFS special/zero-length files and metadata are synced via rsync tunneled through stunnel (port 8873 -> daemon 8874).

Ports:

| Port | Purpose |
|---|---|
| 8000 | stunnel TLS (control channel: cert exchange + version handshake) |
| 8080 | gRPC server (internal, behind stunnel on destination) |
| 8081 | direct TLS gRPC (data transfer: Write/Delete/Commit/Done) |
| 8873 | rsync stunnel port (CephFS only) |
| 8874 | rsync daemon port (CephFS only) |

## gRPC Protocol

`internal/proto/api/v1/sync.proto` defines `SyncService`:

| RPC | Type | Purpose |
|---|---|---|
| `Write` | bidi-stream | Send changed blocks; receive ACK IDs |
| `Commit` | bidi-stream | Finalize written files/blocks |
| `Delete` | bidi-stream | Remove paths on destination |
| `Done` | unary | Signal sync completion |
| `ExchangeCerts` | unary | Exchange ephemeral TLS certificates (over stunnel) |

`ChangedBlock` carries: `request_id`, `file_path`, `total_size`, `offset`, `length`, `is_zero`, and `data` (empty when `is_zero` is true).

**Connection flow:** Version handshake (`GetVersion`) and `ExchangeCerts` run over the stunnel channel (port 8000). `ExchangeCerts` returns the destination's ephemeral certificate and direct TLS port. All data RPCs (`Write`, `Delete`, `Commit`, `Done`) run over the direct TLS channel (port 8081).

## Snapshot Lifecycle

```
Source PVC --> VolumeSnapshot (label: status=current)
                      |
              Temporary PVC (same name as snapshot, mounted in Job)
                      |
               SnapshotDiffer (previous vs current) or full copy
                      |
               [sync completes]
                      |
              current snapshot --> relabeled as previous (kept for next incremental)
              older previous snapshots + temp PVC + Job --> marked for cleanup
```

- `snapshot_method.go` manages VolumeSnapshot creation, label rotation, and cleanup.
- On first sync, no previous snapshot exists and a full copy is performed.
- RBD snapshots restore as Block-mode PVCs regardless of the application VolumeMode; the operator annotates VolumeSnapshotContent with `snapshot.storage.kubernetes.io/allow-volume-mode-change` when mode differs.

## Ceph Configuration Discovery

`internal/ceph/config/` reads the `ceph-csi-config` ConfigMap (namespace: `rook-ceph` by default) to discover Ceph cluster monitors and FSID. This avoids hard-coding cluster topology in the operator.

The ConfigMap name and namespace are configurable via `CEPH_CSI_CONFIG_NAME` and `CEPH_CSI_CONFIG_NAMESPACE` environment variables on the manager pod.
