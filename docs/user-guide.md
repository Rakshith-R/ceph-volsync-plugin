# User Guide

This guide covers installing, configuring, and operating the ceph-volsync-plugin to replicate
Ceph-backed Kubernetes volumes between clusters or namespaces.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuring Replication](#configuring-replication)
  - [CephFS Replication](#cephfs-replication)
  - [RBD Replication](#rbd-replication)
- [Scheduled vs Manual Sync](#scheduled-vs-manual-sync)
- [Monitoring](#monitoring)
- [Troubleshooting](#troubleshooting)
- [Uninstalling](#uninstalling)

---

## Prerequisites

Before deploying the plugin, ensure the following are in place:

| Requirement | Notes |
|---|---|
| Kubernetes ≥ 1.28 | Both source and destination clusters |
| VolSync operator ≥ v0.11.0 | Installed on both clusters |
| Ceph cluster with ceph-csi | At least one supported CSI driver (see below) |
| VolumeSnapshot CRDs + controller | `snapshot.storage.k8s.io/v1` CRDs must be installed |
| StorageClass | Must reference a Ceph CSI provisioner |
| VolumeSnapshotClass | Must match the CSI driver of the source/destination volumes |
| `ceph-csi-config` ConfigMap | Looked up in `rook-ceph` by default; override with `CEPH_CSI_CONFIG_NAMESPACE` |

**Supported CSI providers** (set in `spec.external.provider`):

- `cephfs.csi.ceph.com`
- `nfs.csi.ceph.com`
- `rbd.csi.ceph.com`
- `nvmeof.csi.ceph.com`

---

## Installation

### 1. Install CRDs

```bash
make install
```

### 2. Deploy the Operator

```bash
kubectl create namespace ceph-volsync --dry-run=client -o yaml | kubectl apply -f -

make deploy IMG=quay.io/ramendr/ceph-volsync-plugin-operator:latest \
  MOVER_IMG=quay.io/ramendr/ceph-volsync-plugin-mover:latest
```

By default this deploys to the `ceph-volsync` namespace.

On OpenShift, the operator automatically creates a privileged SCC
(`ceph-volsync-plugin-privileged-mover`) required by mover pods.

#### CSI ConfigMap requirement

The operator needs to read the `ceph-csi-config` ConfigMap to discover Ceph cluster
topology and provisioner secrets. By default it looks for the ConfigMap named
`ceph-csi-config` in the `rook-ceph` namespace.

If the operator is deployed in the **same namespace as the Ceph CSI driver** (e.g.
`rook-ceph`), no extra configuration is needed.

If the operator runs in a **different namespace**, set these environment variables on
the manager Deployment:

| Variable | Description |
|----------|-------------|
| `CEPH_CSI_CONFIG_NAME` | ConfigMap name (default: `ceph-csi-config`) |
| `CEPH_CSI_CONFIG_NAMESPACE` | Namespace where the ConfigMap lives (default: `rook-ceph`) |

The operator must have RBAC permissions to read ConfigMaps and Secrets in the CSI
namespace. By default the generated RBAC is cluster-scoped and covers this.

#### Deploying in a custom namespace

The default kustomize overlay deploys to `ceph-volsync` namespace. To change it,
edit `config/default/kustomization.yaml`:

```yaml
namespace: my-custom-namespace
```

Then deploy:

```bash
make deploy IMG=quay.io/ramendr/ceph-volsync-plugin-operator:latest \
  MOVER_IMG=quay.io/ramendr/ceph-volsync-plugin-mover:latest
```

#### Override the mover image

The operator uses the `MOVER_IMG` Makefile variable (default: versioned tag). Override
with the `MOVER_IMAGE` environment variable on the operator Deployment.

### 3. Verify

```bash
kubectl get pods -n ceph-volsync
```

The controller-manager pod should be in `Running` state.

---

## Configuring Replication

### Ceph Credentials

Ceph credentials are **automatically obtained** by the operator. It reads the `clusterID`
from the StorageClass, looks up the provisioner secret reference in the `ceph-csi-config`
ConfigMap, fetches the secret, and copies `userID`/`userKey` into the mover namespace.

No manual Ceph secret creation is required.

---

### CephFS Replication

Use provider `cephfs.csi.ceph.com` (or `nfs.csi.ceph.com`) for filesystem volumes.

#### Source

```yaml
apiVersion: volsync.backube/v1alpha1
kind: ReplicationSource
metadata:
  name: cephfs-source
  namespace: <namespace>
spec:
  sourcePVC: <source-pvc-name>
  trigger:
    schedule: "*/5 * * * *"
  external:
    provider: cephfs.csi.ceph.com
    parameters:
      copyMethod: Snapshot
      storageClassName: <ceph-cephfs-sc>
      volumeSnapshotClassName: <cephfs-vsc>

      address: <copy from destination status.rsyncTLS.address>
      keySecret: <copy from destination status.rsyncTLS.keySecret>
```

#### Destination

```yaml
apiVersion: volsync.backube/v1alpha1
kind: ReplicationDestination
metadata:
  name: cephfs-destination
  namespace: <namespace>
spec:
  external:
    provider: cephfs.csi.ceph.com
    parameters:
      copyMethod: Snapshot
      storageClassName: <ceph-cephfs-sc>
      volumeSnapshotClassName: <cephfs-vsc>
      destinationPVC: <dest-pvc-name>
```

Apply the destination CR first. The operator creates a Service and auto-generates a PSK
Secret. Copy both into the source CR:
- `status.rsyncTLS.address` -> source `parameters.address`
- `status.rsyncTLS.keySecret` -> source `parameters.keySecret`

For cross-cluster replication, also copy the PSK Secret data into the source namespace.

> **Note:** Source mover pods run with `hostNetwork: true` to reach Ceph daemons on the
> host network. They will not show a cluster IP like normal pods.

---

### RBD Replication

Use provider `rbd.csi.ceph.com` (or `nvmeof.csi.ceph.com`) for block volumes.

RBD replication transfers only changed blocks between snapshots (incremental diff), falling
back to a full transfer when no base snapshot is available.

RBD movers always attach a Block-mode PVC from the snapshot, even if the application PVC
uses Filesystem mode. The operator annotates the VolumeSnapshotContent with
`snapshot.storage.kubernetes.io/allow-volume-mode-change`. The application VolumeMode is
unchanged.

#### Source

```yaml
apiVersion: volsync.backube/v1alpha1
kind: ReplicationSource
metadata:
  name: rbd-source
  namespace: <namespace>
spec:
  sourcePVC: <source-pvc-name>
  trigger:
    schedule: "*/5 * * * *"
  external:
    provider: rbd.csi.ceph.com
    parameters:
      copyMethod: Snapshot
      storageClassName: <ceph-rbd-sc>
      volumeSnapshotClassName: <rbd-vsc>
      address: <copy from destination status.rsyncTLS.address>
      keySecret: <copy from destination status.rsyncTLS.keySecret>
```

#### Destination

```yaml
apiVersion: volsync.backube/v1alpha1
kind: ReplicationDestination
metadata:
  name: rbd-destination
  namespace: <namespace>
spec:
  external:
    provider: rbd.csi.ceph.com
    parameters:
      copyMethod: Snapshot
      storageClassName: <ceph-rbd-sc>
      volumeSnapshotClassName: <rbd-vsc>
      destinationPVC: <dest-pvc-name>
```

---

## Scheduled vs Manual Sync

### Scheduled

Set a cron expression in `spec.trigger.schedule`. The operator will create mover Jobs on
each schedule tick:

```yaml
trigger:
  schedule: "*/15 * * * *"   # every 15 minutes
```

### Manual

Omit `schedule` and set `spec.trigger.manual` to a unique token. Changing the token
triggers a new sync:

```yaml
trigger:
  manual: "sync-2026-06-29-v1"
```

Update the token value whenever you want to start a sync. The operator runs one Job per
unique token value.

---

## Monitoring

### Status Conditions

Inspect `status.conditions` on the CR to see the current state:

```bash
kubectl describe replicationsource cephfs-source -n <namespace>
kubectl describe replicationdestination cephfs-destination -n <namespace>
```

Key status fields: `status.lastSyncDuration`, `status.lastSyncTime`, `status.conditions`.

### Operator Logs

```bash
kubectl logs -n ceph-volsync \
  deploy/ceph-volsync-plugin-operator-controller-manager
```

### Mover Pod Logs

The operator creates short-lived Jobs. Find and inspect mover pods:

```bash
kubectl get pods -n <namespace> -l app.kubernetes.io/part-of=ceph-volsync-plugin
kubectl logs -n <namespace> <mover-pod-name>
```

### Debug Logging

Manager (operator) log verbosity is controlled by zap flags (`--zap-log-level`) on the
manager Deployment.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Mover pod `CrashLoopBackOff` | Missing `ceph-csi-config` ConfigMap or unreachable provisioner secret | Verify ConfigMap exists in Ceph namespace; check provisioner secret is accessible |
| Timeout connecting to destination | Network policy blocking ports 8000/8081 | Allow ingress/egress on TCP 8000 (stunnel) and 8081 (direct TLS) between namespaces |
| `Snapshot not found` error | Missing or mismatched VolumeSnapshotClass | Ensure VSC exists and its driver matches the CSI provisioner |
| `Permission denied` on OpenShift | SCC not applied | Check `ceph-volsync-plugin-privileged-mover` SCC exists; restart the operator if needed |
| No mover Job created | Provider name mismatch | `spec.external.provider` must end with one of the supported CSI driver suffixes |
| Full sync every cycle | Previous snapshot not retained | For Snapshot mode: verify VolumeSnapshots have current/previous status labels. For Direct mode: set `volumeName`, `baseSnapshotName`, `targetSnapshotName` on the CR |

### Increase Verbosity

Manager logs: use `--zap-log-level=debug` on the manager Deployment.

---

## Uninstalling

```bash
make undeploy    # Remove operator Deployment and RBAC
make uninstall   # Remove CRDs
```

Any in-progress replication Jobs will be terminated. Existing PVCs and snapshots are not
automatically deleted.
