/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package constant

// WorkerType represents the role of a mover worker.
type WorkerType string

const (
	// WorkerSource identifies a source-side mover worker.
	WorkerSource WorkerType = "source"
	// WorkerDestination identifies a destination-side mover worker.
	WorkerDestination WorkerType = "destination"
)

// MoverType represents the type of data mover.
type MoverType string

const (
	// MoverCephFS selects the CephFS data mover.
	MoverCephFS MoverType = "cephfs"
	// MoverRBD selects the RBD data mover.
	MoverRBD MoverType = "rbd"
)

// Environment variable key constants for the mover worker
// container. These are passed to Kubernetes Jobs as
// container env var names.
const (
	// EnvMoverType selects the mover backend (cephfs or rbd).
	EnvMoverType = "MOVER_TYPE"
	// EnvWorkerType selects source or destination role.
	EnvWorkerType = "WORKER_TYPE"
	// EnvDestinationPort sets the stunnel TLS port.
	EnvDestinationPort = "DESTINATION_PORT"
	// EnvLogLevel controls worker log verbosity.
	EnvLogLevel = "LOG_LEVEL"
	// EnvRsyncPort sets the rsync listening port.
	EnvRsyncPort = "RSYNC_PORT"
	// EnvRsyncDaemonPort sets the rsync daemon port.
	EnvRsyncDaemonPort = "RSYNC_DAEMON_PORT"
	// EnvPodNamespace passes the pod's namespace.
	EnvPodNamespace = "POD_NAMESPACE"
	// EnvDestinationAddress sets the destination endpoint.
	EnvDestinationAddress = "DESTINATION_ADDRESS"
	// EnvPrivilegedMover enables privileged operations.
	EnvPrivilegedMover = "PRIVILEGED_MOVER"
	// EnvVolumeHandle identifies the CSI volume.
	EnvVolumeHandle = "VOLUME_HANDLE"
	// EnvBaseSnapshotHandle identifies the base snapshot.
	EnvBaseSnapshotHandle = "BASE_SNAPSHOT_HANDLE"
	// EnvTargetSnapshotHandle identifies the target snapshot.
	EnvTargetSnapshotHandle = "TARGET_SNAPSHOT_HANDLE"

	// CsiSecretMountPath is the mount path for the
	// ceph-csi secret volume.
	CsiSecretMountPath = "/etc/ceph-csi-secret" //nolint:gosec // G101: mount path, not credentials
	// CsiSecretUserIDKey is the secret key for the
	// ceph user ID.
	CsiSecretUserIDKey = "userID"
	// CsiSecretUserKeyKey is the secret key for the
	// ceph user key file.
	CsiSecretUserKeyKey = "userKey"

	// TLSPort is the stunnel TLS proxy port.
	TLSPort int32 = 8000

	// RsyncStunnelPort is the rsync stunnel port for
	// CephFS mover workers.
	RsyncStunnelPort int32 = 8873

	// RsyncDaemonPort is the rsync daemon port for
	// CephFS mover workers.
	RsyncDaemonPort int32 = 8874

	// DataMountPath is the mount path for the data
	// PVC inside the mover container.
	DataMountPath = "/data"

	// DevicePath is the block device path for RBD
	// volumes inside the mover container.
	DevicePath = "/dev/block"
)
