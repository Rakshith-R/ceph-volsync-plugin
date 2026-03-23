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

// Environment variable key constants for the mover worker
// container. These are passed to Kubernetes Jobs as
// container env var names.
const (
	EnvWorkerType           = "WORKER_TYPE"
	EnvDestinationPort      = "DESTINATION_PORT"
	EnvLogLevel             = "LOG_LEVEL"
	EnvRsyncPort            = "RSYNC_PORT"
	EnvRsyncDaemonPort      = "RSYNC_DAEMON_PORT"
	EnvPodNamespace         = "POD_NAMESPACE"
	EnvDestinationAddress   = "DESTINATION_ADDRESS"
	EnvPrivilegedMover      = "PRIVILEGED_MOVER"
	EnvVolumeHandle         = "VOLUME_HANDLE"
	EnvBaseSnapshotHandle   = "BASE_SNAPSHOT_HANDLE"
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

	// DefaultServerStunnelPort is the string form of
	// TLSPort for env var configuration.
	DefaultServerStunnelPort = "8000"

	// DefaultRsyncStunnelPort is the string form of
	// RsyncStunnelPort for env var configuration.
	DefaultRsyncStunnelPort = "8873"

	// DefaultRsyncDaemonPort is the string form of
	// RsyncDaemonPort for env var configuration.
	DefaultRsyncDaemonPort = "8874"

	// DataMountPath is the mount path for the data
	// PVC inside the mover container.
	DataMountPath = "/data"

	// DevicePath is the block device path for RBD
	// volumes inside the mover container.
	DevicePath = "/dev/block"
)
