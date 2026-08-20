# ceph-volsync-plugin

A [VolSync](https://volsync.readthedocs.io/) external mover plugin for Ceph storage, enabling efficient snapshot-based volume replication for CephFS and RBD volumes.

[![Build](https://github.com/RamenDR/ceph-volsync-plugin/actions/workflows/build-image.yaml/badge.svg)](https://github.com/RamenDR/ceph-volsync-plugin/actions/workflows/build-image.yaml)
[![Go version](https://img.shields.io/badge/go-1.25-blue)](go.mod)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

## Overview

`ceph-volsync-plugin` extends VolSync with a Ceph-native data mover. Instead of copying entire volumes or relying on generic file-level rsync, it uses Ceph's snapshot diff APIs to identify and transfer only the changed data between replications.

It registers as an external mover with VolSync and handles `ReplicationSource` and `ReplicationDestination` custom resources for PVCs backed by supported Ceph CSI drivers.

**Supported CSI providers:**

| Provider | Mover |
|---|---|
| `cephfs.csi.ceph.com` | CephFS (snapshot diff + rsync fallback) |
| `nfs.csi.ceph.com` | CephFS |
| `rbd.csi.ceph.com` | RBD (block diff) |
| `nvmeof.csi.ceph.com` | RBD |

## Features

- **Incremental sync** — uses CephFS snapshot diff and RBD block diff APIs to transfer only changed data
- **Concurrent pipeline** — parallel gRPC data pipeline with configurable concurrency
- **Direct TLS** — data transfer uses ephemeral mTLS certificates exchanged at startup; stunnel handles only the initial handshake and rsync tunneling
- **Zero-block optimization** — zeroed and unchanged ranges are detected early and never transferred
- **CephFS + RBD** — CephFS routes non-empty regular files through the block pipeline and uses rsync for special files, zero-length files, and metadata; RBD operates directly on raw block devices
- **OpenShift ready** — automatically creates the required `SecurityContextConstraints` on OpenShift clusters

## Architecture

```
  Source Cluster                         Destination Cluster
  ------------------------------         --------------------------------
  ReplicationSource CR                   ReplicationDestination CR
        |                                        |
  ReplicationSource                       ReplicationDestination
    Controller                               Controller
        |                                        |
  Ceph Mover Builder                      Ceph Mover Builder
        |                                        |
  K8s Job                                 K8s Job
  (Source Worker)                         (Destination Worker)
        |                                        |
        |<-- stunnel (handshake) + direct TLS -->|
```

**Data flow:**

1. Controllers watch `ReplicationSource`/`ReplicationDestination` CRs
2. The Ceph mover builder detects the CSI provider from `spec.external.provider` and creates a `Mover`
3. The mover orchestrates Kubernetes Jobs, PVCs, Services, and Secrets
4. Source worker reads snapshot diffs and streams changed blocks via gRPC
5. Destination worker receives and writes blocks, then commits

**CephFS path:** SnapshotDiffer categorises changes into non-empty regular files (block diff pipeline), special/zero-length files (rsync), deletions (gRPC Delete), and directory metadata (convergence rsync) -- all run concurrently.

**RBD path:** RBD diff iterator yields changed block ranges; these flow through a concurrent pipeline to the destination block device. Supports both full (no prior snapshot) and incremental (snapshot-to-snapshot) modes.

## Prerequisites

- Kubernetes ≥ 1.28
- [VolSync](https://volsync.readthedocs.io/en/stable/installation/index.html) installed in the cluster
- Ceph cluster with [ceph-csi](https://github.com/ceph/ceph-csi) drivers deployed
- VolumeSnapshot CRDs and external snapshot controller installed
- A `StorageClass` and `VolumeSnapshotClass` referencing a supported Ceph CSI driver

## Quick Start

### Install from a release (recommended)

```bash
# Set the desired release version
RELEASE=v0.1.0

# Apply the consolidated install manifest
kubectl apply -f "https://github.com/RamenDR/ceph-volsync-plugin/releases/download/${RELEASE}/install.yaml"
```

The install manifest includes CRDs, RBAC, and the operator deployment with versioned
container image references baked in. By default it deploys into the `ceph-volsync`
namespace and reads the Ceph CSI config from `ceph-csi-config` ConfigMap in `rook-ceph`.

#### Customizing the release manifest

To change the namespace or CSI config location, download and patch the manifest:

```bash
export RELEASE=v0.1.0
export NAMESPACE=my-namespace
export CEPH_CSI_CONFIG_NAME=my-csi-configmap
export CEPH_CSI_CONFIG_NAMESPACE=my-ceph-namespace

curl -sL "https://github.com/RamenDR/ceph-volsync-plugin/releases/download/${RELEASE}/install.yaml" \
  | sed "s/namespace: ceph-volsync/namespace: ${NAMESPACE}/g" \
  | sed "s/value: ceph-csi-config/value: ${CEPH_CSI_CONFIG_NAME}/g" \
  | sed "s/value: rook-ceph/value: ${CEPH_CSI_CONFIG_NAMESPACE}/g" \
  | kubectl apply -f -
```

| Variable | Default | Sed target | Description |
|----------|---------|-----------|-------------|
| `NAMESPACE` | `ceph-volsync` | `namespace: ceph-volsync` | Operator deployment namespace |
| `CEPH_CSI_CONFIG_NAME` | `ceph-csi-config` | `value: ceph-csi-config` | Ceph CSI ConfigMap name |
| `CEPH_CSI_CONFIG_NAMESPACE` | `rook-ceph` | `value: rook-ceph` | Ceph CSI ConfigMap namespace |

### Install from source

For full control over all configuration, build and deploy from source.

To change the deployment namespace, edit `namespace:` in `config/default/kustomization.yaml`.
To change the CSI ConfigMap name or namespace, pass them as Make variables:

```bash
# Install CRDs (VolSync CRDs must already be present)
make install

# Deploy with custom CSI config
make deploy \
  IMG=quay.io/ramendr/ceph-volsync-plugin-operator:latest \
  MOVER_IMG=quay.io/ramendr/ceph-volsync-plugin-mover:latest \
  CEPH_CSI_CONFIG_NAME=my-csi-configmap \
  CEPH_CSI_CONFIG_NAMESPACE=my-ceph-namespace
```

### Configure replication

Ceph credentials are automatically obtained from the CSI provisioner secret
referenced in the `ceph-csi-config` ConfigMap — no manual secret creation required.

Create a `ReplicationSource` on the source cluster:

```yaml
apiVersion: volsync.backube/v1alpha1
kind: ReplicationSource
metadata:
  name: my-source
  namespace: my-namespace
spec:
  sourcePVC: my-pvc
  trigger:
    schedule: "*/30 * * * *"  # every 30 minutes
  external:
    provider: cephfs.csi.ceph.com
    parameters:
      storageClassName: cephfs-sc
      volumeSnapshotClassName: cephfs-snapclass
      copyMethod: Snapshot
      address: <copy from destination status.rsyncTLS.address>
```

Create a `ReplicationDestination` on the destination cluster:

```yaml
apiVersion: volsync.backube/v1alpha1
kind: ReplicationDestination
metadata:
  name: my-dest
  namespace: my-namespace
spec:
  trigger:
    manual: first-sync
  external:
    provider: cephfs.csi.ceph.com
    parameters:
      storageClassName: cephfs-sc
      volumeSnapshotClassName: cephfs-snapclass
      copyMethod: Snapshot
      destinationPVC: my-dest-pvc
```

See [docs/user-guide.md](docs/user-guide.md) for a complete walkthrough including RBD volumes and incremental sync.

## Configuration

### CR external parameters

Set these in `spec.external.parameters` on both `ReplicationSource` and `ReplicationDestination`.

| Parameter | Source | Dest | Description |
|---|:---:|:---:|---|
| `storageClassName` | ✓ | ✓ | StorageClass for the snapshot PVC |
| `volumeSnapshotClassName` | ✓ | ✓ | VolumeSnapshotClass for creating snapshots |
| `copyMethod` | ✓ | ✓ | `Snapshot` (recommended) or `Direct`; defaults to `Direct` if omitted |
| `address` | ✓ | | Destination service address (hostname or IP) |
| `keySecret` | ✓ | ✓ | Stunnel PSK Secret (`psk.txt`). Destination auto-generates one; source must reference it via `status.rsyncTLS.keySecret`. For cross-cluster, copy the Secret data to the source namespace. |
| `destinationPVC` | | ✓ (required) | Pre-existing destination PVC name |
| `volumeName` | ✓ | | PVC name to resolve CSI volume handle (source + Direct only) |
| `baseSnapshotName` | ✓ | | Base VolumeSnapshot name for incremental diff (source + Direct only) |
| `targetSnapshotName` | ✓ | | Target VolumeSnapshot name (source + Direct only) |

> `volumeName`, `baseSnapshotName`, and `targetSnapshotName` are used only on the source with `copyMethod: Direct`. Snapshot mode tracks incremental state via snapshot status labels.

### Operator environment variables

| Variable | Default | Description |
|---|---|---|
| `MOVER_IMAGE` | `quay.io/ramendr/ceph-volsync-plugin-mover:latest` | Override the mover container image |
| `CEPH_CSI_CONFIG_NAME` | `ceph-csi-config` | ConfigMap name for Ceph CSI cluster config |
| `CEPH_CSI_CONFIG_NAMESPACE` | `rook-ceph` | Namespace of the Ceph CSI config ConfigMap |

See [docs/configuration.md](docs/configuration.md) for the full reference including mover environment variables and manager CLI flags.

## Building

Protobuf generation is required before the first build:

```bash
make proto-generate        # generate gRPC/protobuf code (required first)
make generate              # generate deepcopy methods
make manifests             # generate CRDs and RBAC
make build                 # build manager binary
```

Container images:

```bash
make docker-build IMG=<registry>/ceph-volsync-plugin-operator:<tag>
make docker-build-mover MOVER_IMG=<registry>/ceph-volsync-plugin-mover:<tag>

# Multi-arch builds
make docker-buildx IMG=<registry>/ceph-volsync-plugin-operator:<tag>
make docker-buildx-mover MOVER_IMG=<registry>/ceph-volsync-plugin-mover:<tag>
```

**Pinned versions:** Go 1.25.7 · Ceph tentacle (`quay.io/ceph/ceph:v20`) · rsync 3.2.5 · stunnel 5.71

## Testing

```bash
make test          # unit tests (uses envtest)
make test-e2e      # end-to-end tests (Kind cluster with Rook Ceph)
make lint          # golangci-lint
make lint-fix      # auto-fix linting issues
```

Local e2e tests use Kind (`make test-e2e`). CI uses Minikube. Both deploy Rook Ceph, install the snapshot controller, build and deploy the operator, then run the Ginkgo test suite. Requires Docker.

## Project Structure

```
ceph-volsync-plugin/
├── cmd/
│   ├── manager/        # Kubernetes operator entry point
│   └── mover/          # Mover pod entry point (worker factory)
├── internal/
│   ├── ceph/           # go-ceph wrappers (CephFS diff, RBD diff, config)
│   ├── controller/     # ReplicationSource and ReplicationDestination controllers
│   ├── mover/ceph/     # Mover builder and K8s Job orchestration
│   ├── proto/          # gRPC/protobuf definitions (SyncService, VersionService)
│   └── worker/         # Data transfer workers
│       ├── cephfs/     # CephFS source and destination workers
│       ├── rbd/        # RBD source and destination workers
│       ├── pipeline/   # Concurrent Read->SendData pipeline
│       ├── tunnel/     # stunnel + rsync daemon management
│       ├── common/     # Shared worker interfaces and gRPC infrastructure
│       └── constant/   # Env var names, ports, paths
├── config/             # Kustomize: CRDs, RBAC, manager deployment
├── build/              # Containerfiles and build.env (pinned versions)
└── test/               # E2E tests and deployment scripts
```

## Documentation

- [Architecture](docs/architecture.md) — component diagram, data flow, pipeline internals
- [User Guide](docs/user-guide.md) — deployment, CephFS and RBD walkthroughs, troubleshooting
- [Configuration Reference](docs/configuration.md) — all parameters, env vars, and CLI flags
- [Contributing](CONTRIBUTING.md) — dev setup, code generation workflow, PR process

## License

Apache License 2.0 — see [LICENSE](LICENSE).
