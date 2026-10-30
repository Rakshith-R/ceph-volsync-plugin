# Configuration Reference

## CR External Parameters

Set in `spec.external.parameters` of `ReplicationSource` and `ReplicationDestination` CRs.

### Common Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `storageClassName` | string | No | — | StorageClass for temporary PVCs created from snapshots |
| `volumeSnapshotClassName` | string | No | — | VolumeSnapshotClass for creating snapshots |
| `copyMethod` | string | No | `Direct` | Copy method: `Snapshot` (recommended) or `Direct` |
| `keySecret` | string | No | auto-generated | Stunnel PSK Secret (`psk.txt`). Destination auto-generates one; source must reference it via `status.rsyncTLS.keySecret`. For cross-cluster, copy the Secret data to the source namespace. |

### Source-Only Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `address` | string | Yes | — | Hostname/IP of the destination service for gRPC connection |

### Destination-Only Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `destinationPVC` | string | Yes | — | Name of pre-existing destination PVC (required; dynamic allocation not currently supported) |

### Source-Only Parameters (Direct Copy Incremental)

Used on the source with `copyMethod: Direct` for incremental sync. Not needed with `copyMethod: Snapshot` (state tracked via snapshot labels). Destination does not read these.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `volumeName` | string | No | — | PVC name used to resolve CSI volume handle |
| `baseSnapshotName` | string | No | — | Base VolumeSnapshot name for incremental diff |
| `targetSnapshotName` | string | No | — | Target VolumeSnapshot name |

---

## Supported CSI Providers

Set in `spec.external.provider`. The plugin selects CephFS or RBD mover based on the provider suffix.

| Provider | Mover Type | Description |
|----------|-----------|-------------|
| `cephfs.csi.ceph.com` | CephFS | CephFS filesystem volumes |
| `nfs.csi.ceph.com` | CephFS | CephFS volumes exported via NFS |
| `rbd.csi.ceph.com` | RBD | RBD block device volumes |
| `nvmeof.csi.ceph.com` | RBD | RBD volumes via NVMe-oF |

---

## Operator Environment Variables

Set on the manager Deployment. Override defaults via kustomize patches on the manager Deployment (no Helm chart in this repository).

| Variable | Default | Description |
|----------|---------|-------------|
| `MOVER_IMAGE` | `quay.io/ramendr/ceph-volsync-plugin-mover:latest` | Override mover container image |
| `CEPH_CSI_CONFIG_NAME` | `ceph-csi-config` | ConfigMap name for Ceph cluster config discovery |
| `CEPH_CSI_CONFIG_NAMESPACE` | `rook-ceph` | Namespace of the Ceph CSI ConfigMap |

---

## Mover Environment Variables

Set automatically by the operator on mover Kubernetes Jobs. Documented here for debugging and advanced use.

| Variable | Values | Default | Description |
|----------|--------|---------|-------------|
| `MOVER_TYPE` | `cephfs`, `rbd` | `cephfs` | Selects mover backend |
| `WORKER_TYPE` | `source`, `destination` | — (required) | Worker role |
| `DESTINATION_ADDRESS` | hostname/IP | — | Destination service endpoint |
| `DESTINATION_PORT` | port number | `8000` | Stunnel TLS port |
| `LOG_LEVEL` | `debug`, `info`, `warn`, `error` | `info` | Log verbosity |
| `RSYNC_PORT` | port number | `8873` | Rsync stunnel port (CephFS only) |
| `RSYNC_DAEMON_PORT` | port number | `8874` | Rsync daemon port (CephFS only) |
| `POD_NAMESPACE` | namespace | — | Pod's Kubernetes namespace |
| `PRIVILEGED_MOVER` | `1`, `0` | set by operator | `1` adds Linux capabilities (DAC_OVERRIDE, CHOWN, FOWNER, SETGID) and RunAsUser 0 |
| `VOLUME_HANDLE` | CSI volume ID | — | CSI volume handle identifying the source volume |
| `BASE_SNAPSHOT_HANDLE` | CSI snapshot ID | — | Base snapshot handle for incremental sync |
| `TARGET_SNAPSHOT_HANDLE` | CSI snapshot ID | — | Target snapshot handle |

---

## Manager CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--metrics-bind-address` | `0` (disabled) | Metrics endpoint bind address; use `:8443` for HTTPS or `:8080` for HTTP |
| `--health-probe-bind-address` | `:8081` | Health/readiness probe bind address |
| `--leader-elect` | `false` | Enable leader election for HA deployments |
| `--metrics-secure` | `true` | Serve metrics over HTTPS |
| `--enable-http2` | `false` | Enable HTTP/2 (disabled by default to mitigate stream cancellation CVEs) |
| `--mover-image` | `quay.io/ramendr/ceph-volsync-plugin-mover:latest` | Mover container image (flag registered but not bound to Viper; use `MOVER_IMAGE` env var instead) |
| `--webhook-cert-path` | — | Directory containing the webhook TLS certificate |
| `--webhook-cert-name` | `tls.crt` | Webhook certificate filename |
| `--webhook-cert-key` | `tls.key` | Webhook key filename |
| `--metrics-cert-path` | — | Directory containing the metrics server TLS certificate |
| `--metrics-cert-name` | `tls.crt` | Metrics certificate filename |
| `--metrics-cert-key` | `tls.key` | Metrics key filename |

Standard `zap` logging flags (`--zap-log-level`, `--zap-encoder`, etc.) are also available.

---

## Network Ports

| Port | Protocol | Component | Description |
|------|----------|-----------|-------------|
| 8000 | TCP/TLS | Mover | Stunnel TLS (control channel: cert exchange, version handshake) |
| 8080 | TCP | Mover | gRPC server (internal, behind stunnel on destination) |
| 8081 | TCP/mTLS | Mover pod | Direct TLS gRPC (data transfer: Write/Delete/Commit/Done) |
| 8081 | HTTP | Manager pod | Health (`/healthz`) and readiness (`/readyz`) probes |
| 8873 | TCP/TLS | Mover (CephFS) | Rsync stunnel port |
| 8874 | TCP | Mover (CephFS) | Rsync daemon port |

Port 8081 is used by different pods - mover pods use it for direct TLS data transfer,
while the manager pod uses it for health probes. No runtime conflict.

---

## Mover Container Paths

| Path | Description |
|------|-------------|
| `/data` | Mount path for the data PVC (CephFS volumes) |
| `/dev/block` | Block device path for RBD volumes |
| `/etc/ceph-csi-secret` | Ceph CSI secret mount (`userID`, `userKey`) |
