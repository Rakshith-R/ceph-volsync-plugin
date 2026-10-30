# controller

Kubernetes controllers for VolSync `ReplicationSource` and `ReplicationDestination` custom resources.

## Overview

This package contains two reconcilers that watch VolSync CRs and drive the Ceph data mover
lifecycle. Each reconciler delegates business logic to a mover selected from the catalog
registered in `cmd/manager/main.go`.

## Controllers

### ReplicationSourceReconciler

Watches `ReplicationSource` CRs and creates source-side mover Jobs that read from the source
PVC and stream changed blocks to the destination.

- Indexed on `spec.sourcePVC` (`ReplicationSourceToSourcePVCIndex`) for efficient lookups when
  a PVC changes.
- Skips CRs that select a built-in VolSync mover (`spec.rclone`, `spec.restic`, `spec.rsync`, or `spec.rsyncTLS`) via the `rsHasMover` check.

### ReplicationDestinationReconciler

Watches `ReplicationDestination` CRs and creates destination-side mover Jobs plus a Service
that the source pod connects to.

## State Machine

Both controllers use VolSync's `statemachine.ReplicationMachine` interface via inner structs
(`rsMachine` / `rdMachine`). VolSync `statemachine.Run` drives:

1. **Synchronize** — `Mover.Synchronize` ensures PVC, Service, PSK secret, ServiceAccount/RBAC, copied ceph-csi ConfigMap/Secret, then the Job.
2. **Cleanup** — `Mover.Cleanup` rotates snapshot labels and deletes objects marked for cleanup.

The mover instance is obtained via `GetSourceMoverFromCatalog` / `GetDestinationMoverFromCatalog`.

## Watched Resources

Controllers watch and reconcile:

| API Group | Resource |
|---|---|
| `volsync.backube` | ReplicationSource, ReplicationDestination |
| `batch` | Jobs |
| `core` | PersistentVolumeClaims, PersistentVolumes, Secrets, Services, ServiceAccounts, ConfigMaps, Namespaces, Pods (logs) |
| `rbac.authorization.k8s.io` | Roles, RoleBindings |
| `snapshot.storage.k8s.io` | VolumeSnapshots, VolumeSnapshotContents |
| `storage.k8s.io` | StorageClasses |
| `security.openshift.io` | SecurityContextConstraints |

RBAC permissions are generated from kubebuilder markers in the source files and materialized
via `make manifests`.

## Key Files

| File | Purpose |
|---|---|
| `replicationsource_controller.go` | ReplicationSource reconcile loop and state machine wiring |
| `replicationdestination_controller.go` | ReplicationDestination reconcile loop and state machine wiring |
