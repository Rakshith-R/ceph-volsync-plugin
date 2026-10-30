# mover/ceph

Ceph mover implementation — the bridge between Kubernetes controllers and mover worker pods.

## Overview

This package implements VolSync's `mover.Builder` and `mover.Mover` interfaces for Ceph
storage backends. It translates `ReplicationSource` / `ReplicationDestination` CRs into
Kubernetes Jobs that run the mover worker container.

## Components

### Builder (`builder.go`)

Implements `mover.Builder`. Registered in `cmd/manager/main.go` via `Register()`.

- `Register()` — creates a `Builder` with global viper/flags and adds it to the mover catalog.
- `FromSource()` — inspects `spec.external.provider` to select CephFS or RBD, then constructs
  a `Mover` configured for the source role.
- `FromDestination()` — same provider detection for the destination role.
- Provider detection:
  - `cephfs.csi.ceph.com`, `nfs.csi.ceph.com` → `MoverCephFS`
  - `rbd.csi.ceph.com`, `nvmeof.csi.ceph.com` → `MoverRBD`
- Mover container image resolved via `MOVER_IMAGE` env var, defaulting to
  `quay.io/ramendr/ceph-volsync-plugin-mover:latest`. (`--mover-image` flag is registered
  but not bound to Viper; use the env var.)

### Mover (`mover.go`)

Implements `mover.Mover`. Drives the sync lifecycle each reconcile iteration:

1. Ensure VolumeSnapshot and temporary PVC (source) or destination PVC (destination)
2. Ensure Ceph CSI secret is copied into the namespace
3. Ensure stunnel PSK secret (auto-generated or from `keySecret` param)
4. Ensure Service (destination only, ports 8000 stunnel + 8081 direct TLS + optional rsync)
5. Ensure ServiceAccount and RBAC
6. Ensure mover Job; return `InProgress` until Job completes
7. Cleanup temporary resources on success

### Job (`job.go`)

Builds the `batchv1.Job` spec:

- Container name: `cephfs-mover` or `rbd-mover`
- Mounts: data PVC at `/data`, Ceph CSI secret at `/etc/ceph-csi-secret`, Ceph CSI ConfigMap
- Env vars: `WORKER_TYPE`, `MOVER_TYPE`, `DESTINATION_ADDRESS`, `VOLUME_HANDLE`,
  `BASE_SNAPSHOT_HANDLE`, `TARGET_SNAPSHOT_HANDLE`, `LOG_LEVEL`, ports
- Security context: catalog currently constructs movers with privileged=true. When privileged, the Job sets `PRIVILEGED_MOVER=1`, RunAsUser 0, and adds DAC_OVERRIDE, CHOWN, FOWNER, SETGID capabilities. Applies to both CephFS and RBD Jobs.

### PVC (`pvc.go`)

Manages PVC lifecycle:
- Source: creates a temporary PVC from a `VolumeSnapshot` via `VolumeHandler`
- Destination: uses an existing PVC named in `spec.external.parameters.destinationPVC`
  (required; dynamic PVC allocation is not currently supported)

### Service (`service.go`)

Creates a `ClusterIP` Service for destination mover pods:
- Port 8000 (stunnel TLS — control channel: cert exchange, version handshake)
- Port 8081 (direct TLS — data transfer: Write/Delete/Commit/Done)
- Optional rsync port 8873 for CephFS movers

### Secrets (`secrets.go`)

- `ensureCephCSISecret()` — auto-fetches Ceph credentials from the CSI provisioner secret.
  Reads `clusterID` from the StorageClass, looks up the secret reference in `ceph-csi-config`,
  fetches the original secret, and copies `userID`/`userKey` into the mover namespace.
- `ensureSecrets()` — manages the stunnel PSK secret. If `keySecret` is set in the CR,
  validates it contains `psk.txt`. Otherwise auto-generates a random PSK and publishes
  the secret name in `status.rsyncTLS.keySecret` so the source can retrieve it.

### Snapshot Method (`snapshot_method.go`)

Manages the `VolumeSnapshot` lifecycle for the copy-method=snapshot path:
- Creates `VolumeSnapshot` from the source PVC
- Waits for `readyToUse=true`
- After sync: relabels current snapshot as previous (kept for next incremental); older previous snapshots and temp PVC are marked for cleanup
- RBD: always restores as Block-mode PVC; annotates VolumeSnapshotContent with `snapshot.storage.kubernetes.io/allow-volume-mode-change` when app PVC is Filesystem

## CR Parameters

All parameters are passed via `spec.external.parameters`:

| Parameter | Source | Destination | Description |
|---|---|---|---|
| `storageClassName` | optional | optional | StorageClass for the temporary PVC |
| `volumeSnapshotClassName` | optional | optional | VolumeSnapshotClass for snapshots |
| `copyMethod` | optional | optional | `Snapshot` (recommended) or `Direct` (default if omitted) |
| `address` | required on source | — | Destination Service hostname/IP. Copy from destination `status.rsyncTLS.address`. |
| `keySecret` | required | optional | Stunnel PSK Secret. Dest auto-generates; source must reference dest's `status.rsyncTLS.keySecret`. |
| `destinationPVC` | — | required | Pre-existing destination PVC name (dynamic allocation not supported) |
| `volumeName` | optional (Direct) | — | PVC name to resolve CSI volume handle (source + Direct only) |
| `baseSnapshotName` | optional (Direct) | — | Base VolumeSnapshot name for incremental diff (source + Direct only) |
| `targetSnapshotName` | optional (Direct) | — | Target VolumeSnapshot name (source + Direct only) |
