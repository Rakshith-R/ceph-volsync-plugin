/*
Copyright 2025.

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

package common

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker"
)

// ReadMountedCredentials reads ceph admin credentials
// from a JSON file mounted at
// /etc/ceph-csi-secret/credentials.json.
func ReadMountedCredentials() (
	*ceph.Credentials, error,
) {
	path := filepath.Join(
		worker.CsiSecretMountPath,
		worker.CsiSecretJSONKey,
	)
	content, err := os.ReadFile(path) //nolint:gosec // G304: path is internally constructed
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read %s: %w", path, err,
		)
	}

	data := map[string]string{}
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf(
			"failed to parse %s: %w", path, err,
		)
	}

	return ceph.NewAdminCredentials(data)
}
