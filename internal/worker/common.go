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

package worker

// Environment variable key constants for the mover worker
// container. These are passed to Kubernetes Jobs as
// container env var names.
const (
	EnvWorkerType           = "WORKER_TYPE"
	EnvDestinationPort      = "DESTINATION_PORT"
	EnvServerPort           = "SERVER_PORT"
	EnvLogLevel             = "LOG_LEVEL"
	EnvEnableRsyncTunnel    = "ENABLE_RSYNC_TUNNEL"
	EnvRsyncPort            = "RSYNC_PORT"
	EnvRsyncDaemonPort      = "RSYNC_DAEMON_PORT"
	EnvPodNamespace         = "POD_NAMESPACE"
	EnvDestinationAddress   = "DESTINATION_ADDRESS"
	EnvPrivilegedMover      = "PRIVILEGED_MOVER"
	EnvVolumeHandle         = "VOLUME_HANDLE"
	EnvBaseSnapshotHandle   = "BASE_SNAPSHOT_HANDLE"
	EnvTargetSnapshotHandle = "TARGET_SNAPSHOT_HANDLE"
)
