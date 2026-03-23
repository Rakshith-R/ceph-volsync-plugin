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

package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ceph/ceph-csi/api/deploy/kubernetes"
)

var (
	// ErrClusterIDNotSet is returned when cluster id is not set.
	ErrClusterIDNotSet = errors.New("clusterID must be set")
	// ErrConfigNotFound is returned when no configuration is found
	// for a cluster ID.
	ErrConfigNotFound = errors.New("missing configuration for cluster ID")
)

const (
	// defaultCsiSubvolumeGroup defines the default name for the
	// CephFS CSI subvolumegroup. This was hardcoded once and
	// defaults to the old value to keep backward compatibility.
	defaultCsiSubvolumeGroup = "csi"

	// defaultCsiCephFSRadosNamespace defines the default RADOS
	// namespace used for storing CSI-specific objects and keys for
	// CephFS volumes.
	defaultCsiCephFSRadosNamespace = "csi"

	// CsiConfigFile is the location of the CSI config file.
	CsiConfigFile = "/etc/ceph-csi-config/config.json"

	// ClusterIDKey is the name of the key containing clusterID.
	ClusterIDKey = "clusterID"
)

// Expected JSON structure in the passed in config file is,
//
//nolint:godot // example json content should not contain unwanted dot.
/*
[{
	"clusterID": "<cluster-id>",
	"rbd": {
		"radosNamespace": "<rados-namespace>"
		"mirrorDaemonCount": 1
	},
	"monitors": [
		"<monitor-value>",
		"<monitor-value>"
	],
	"cephFS": {
		"subvolumeGroup": "<subvolumegroup for cephfs volumes>"
	}
}]
*/
func readClusterInfo(pathToConfig, clusterID string) (*kubernetes.ClusterInfo, error) {
	var config []kubernetes.ClusterInfo

	// #nosec
	content, err := os.ReadFile(pathToConfig)
	if err != nil {
		err = fmt.Errorf("error fetching configuration for cluster ID %q: %w", clusterID, err)

		return nil, err
	}

	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("unmarshal failed (%w), raw buffer response: %s", err, string(content))
	}

	for i := range config {
		if config[i].ClusterID == clusterID {
			return &config[i], nil
		}
	}

	return nil, fmt.Errorf("%w: %q", ErrConfigNotFound, clusterID)
}

// Mons returns a comma separated MON list from the csi config
// for the given clusterID.
func Mons(pathToConfig, clusterID string) (string, error) {
	cluster, err := readClusterInfo(pathToConfig, clusterID)
	if err != nil {
		return "", err
	}

	if len(cluster.Monitors) == 0 {
		return "", fmt.Errorf("empty monitor list for cluster ID (%s) in config", clusterID)
	}

	return strings.Join(cluster.Monitors, ","), nil
}

// GetRBDRadosNamespace returns the namespace for the given clusterID.
func GetRBDRadosNamespace(pathToConfig, clusterID string) (string, error) {
	cluster, err := readClusterInfo(pathToConfig, clusterID)
	if err != nil {
		return "", err
	}

	return cluster.RBD.RadosNamespace, nil
}

// GetCephFSRadosNamespace returns the namespace for the given clusterID.
// If not set, it returns the default value "csi".
func GetCephFSRadosNamespace(pathToConfig, clusterID string) (string, error) {
	cluster, err := readClusterInfo(pathToConfig, clusterID)
	if err != nil {
		return "", err
	}

	if cluster.CephFS.RadosNamespace == "" {
		return defaultCsiCephFSRadosNamespace, nil
	}

	return cluster.CephFS.RadosNamespace, nil
}

// CephFSSubvolumeGroup returns the subvolumeGroup for CephFS volumes.
// If not set, it returns the default value "csi".
func CephFSSubvolumeGroup(pathToConfig, clusterID string) (string, error) {
	cluster, err := readClusterInfo(pathToConfig, clusterID)
	if err != nil {
		return "", err
	}

	if cluster.CephFS.SubvolumeGroup == "" {
		return defaultCsiSubvolumeGroup, nil
	}

	return cluster.CephFS.SubvolumeGroup, nil
}

// GetClusterID fetches clusterID from given options map.
func GetClusterID(options map[string]string) (string, error) {
	clusterID, ok := options[ClusterIDKey]
	if !ok {
		return "", ErrClusterIDNotSet
	}

	return clusterID, nil
}

// GetRBDControllerPublishSecretRef returns the secret name and
// namespace used for controller publish operations for RBD volumes.
func GetRBDControllerPublishSecretRef(pathToConfig, clusterID string) (string, string, error) {
	cluster, err := readClusterInfo(pathToConfig, clusterID)
	if err != nil {
		return "", "", err
	}

	secretRef := cluster.RBD.ControllerPublishSecretRef

	return secretRef.Name, secretRef.Namespace, nil
}

// GetCephFSControllerPublishSecretRef returns the secret name and
// namespace used for controller publish operations for CephFS volumes.
func GetCephFSControllerPublishSecretRef(pathToConfig, clusterID string) (string, string, error) {
	cluster, err := readClusterInfo(pathToConfig, clusterID)
	if err != nil {
		return "", "", err
	}

	secretRef := cluster.CephFS.ControllerPublishSecretRef

	return secretRef.Name, secretRef.Namespace, nil
}
