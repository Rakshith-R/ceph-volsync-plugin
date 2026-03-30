/*
Copyright 2018 The Ceph-CSI Authors.

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

package cephconf

import (
	"os"
)

var cephConfig = []byte(`[global]
auth_cluster_required = cephx
auth_service_required = cephx
auth_client_required = cephx

# Prevent indefinite hangs on OSD/MDS operations
rados_osd_op_timeout = 30
rados_mon_op_timeout = 30
client_mount_timeout = 30
`)

const (
	cephConfigRoot = "/etc/ceph"
	// CephConfigPath ceph configuration file.
	CephConfigPath = "/etc/ceph/ceph.conf"

	keyRing = "/etc/ceph/keyring"
)

func createCephConfigRoot() error {
	return os.MkdirAll(cephConfigRoot, 0o755) // #nosec
}

// WriteCephConfig writes out a basic ceph.conf file,
// making it easy to use ceph related CLIs.
func WriteCephConfig() error {
	var err error
	if err = createCephConfigRoot(); err != nil {
		return err
	}

	if _, err = os.Stat(CephConfigPath); os.IsNotExist(err) {
		err = os.WriteFile(
			CephConfigPath, cephConfig, 0o600,
		)
	}

	if err != nil {
		return err
	}

	return createKeyRingFile()
}

func createKeyRingFile() error {
	var err error
	if _, err = os.Stat(keyRing); os.IsNotExist(err) {
		err = os.WriteFile(keyRing, nil, 0o600)
	}

	return err
}
