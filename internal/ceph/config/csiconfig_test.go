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
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"

	cephcsi "github.com/ceph/ceph-csi/api/deploy/kubernetes"
)

var (
	csiClusters = "csi-clusters.json"
	clusterID1  = "test1"
	clusterID2  = "test2"
)

func TestCSIConfig(t *testing.T) {
	t.Parallel()
	var err error
	var data string
	var content string

	basePath := t.TempDir() + "/test_artifacts"
	pathToConfig := basePath + "/" + csiClusters

	err = os.MkdirAll(basePath, 0o700)
	if err != nil {
		t.Errorf("Test setup error %s", err)
	}

	// TEST: Should fail as clusterid file is missing
	_, err = Mons(pathToConfig, clusterID1)
	if err == nil {
		t.Errorf("Failed: expected error due to missing config")
	}

	data = ""
	err = os.WriteFile(basePath+"/"+csiClusters, []byte(data), 0o600)
	if err != nil {
		t.Errorf("Test setup error %s", err)
	}

	// TEST: Should fail as file is empty
	content, err = Mons(pathToConfig, clusterID1)
	if err == nil {
		t.Errorf("Failed: want (%s), got (%s)", data, content)
	}

	data = "[{\"clusterIDBad\":\"" + clusterID2 + "\",\"monitors\":[\"mon1\",\"mon2\",\"mon3\"]}]"
	err = os.WriteFile(basePath+"/"+csiClusters, []byte(data), 0o600)
	if err != nil {
		t.Errorf("Test setup error %s", err)
	}

	// TEST: Should fail as clusterID data is malformed
	content, err = Mons(pathToConfig, clusterID2)
	if err == nil {
		t.Errorf("Failed: want (%s), got (%s)", data, content)
	}

	data = "[{\"clusterID\":\"" + clusterID2 + "\",\"monitorsBad\":[\"mon1\",\"mon2\",\"mon3\"]}]"
	err = os.WriteFile(basePath+"/"+csiClusters, []byte(data), 0o600)
	if err != nil {
		t.Errorf("Test setup error %s", err)
	}

	// TEST: Should fail as monitors key is incorrect/missing
	content, err = Mons(pathToConfig, clusterID2)
	if err == nil {
		t.Errorf("Failed: want (%s), got (%s)", data, content)
	}

	data = "[{\"clusterID\":\"" + clusterID2 + "\",\"monitors\":[\"mon1\",2,\"mon3\"]}]"
	err = os.WriteFile(basePath+"/"+csiClusters, []byte(data), 0o600)
	if err != nil {
		t.Errorf("Test setup error %s", err)
	}

	// TEST: Should fail as monitor data is malformed
	content, err = Mons(pathToConfig, clusterID2)
	if err == nil {
		t.Errorf("Failed: want (%s), got (%s)", data, content)
	}

	data = "[{\"clusterID\":\"" + clusterID2 + "\",\"monitors\":[\"mon1\",\"mon2\",\"mon3\"]}]"
	err = os.WriteFile(basePath+"/"+csiClusters, []byte(data), 0o600)
	if err != nil {
		t.Errorf("Test setup error %s", err)
	}

	// TEST: Should fail as clusterID is not present in config
	content, err = Mons(pathToConfig, clusterID1)
	if err == nil {
		t.Errorf("Failed: want (%s), got (%s)", data, content)
	}

	// TEST: Should pass as clusterID is present in config
	content, err = Mons(pathToConfig, clusterID2)
	if err != nil || content != "mon1,mon2,mon3" {
		t.Errorf("Failed: want (%s), got (%s) (%v)", "mon1,mon2,mon3", content, err)
	}

	data = "[{\"clusterID\":\"" + clusterID2 + "\",\"monitors\":[\"mon1\",\"mon2\",\"mon3\"]}," +
		"{\"clusterID\":\"" + clusterID1 + "\",\"monitors\":[\"mon4\",\"mon5\",\"mon6\"]}]"
	err = os.WriteFile(basePath+"/"+csiClusters, []byte(data), 0o600)
	if err != nil {
		t.Errorf("Test setup error %s", err)
	}

	// TEST: Should pass as clusterID is present in config
	content, err = Mons(pathToConfig, clusterID1)
	if err != nil || content != "mon4,mon5,mon6" {
		t.Errorf("Failed: want (%s), got (%s) (%v)", "mon4,mon5,mon6", content, err)
	}

	data = "[{\"clusterID\":\"" + clusterID2 + "\",\"monitors\":[\"mon1\",\"mon2\",\"mon3\"]}," +
		"{\"clusterID\":\"" + clusterID1 + "\",\"monitors\":[\"mon4\",\"mon5\",\"mon6\"]}]"
	err = os.WriteFile(basePath+"/"+csiClusters, []byte(data), 0o600)
	if err != nil {
		t.Errorf("Test setup error %s", err)
	}
}

func TestReadClusterInfoFromData(t *testing.T) {
	t.Parallel()
	data := []byte(`[{"clusterID":"c1","monitors":["mon1","mon2"]},{"clusterID":"c2","monitors":["mon3"]}]`)

	info, err := ReadClusterInfoFromData(data, "c1")
	if err != nil {
		t.Fatalf("ReadClusterInfoFromData() error: %v", err)
	}
	if info.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", info.ClusterID, "c1")
	}
	if len(info.Monitors) != 2 {
		t.Errorf("Monitors count = %d, want 2", len(info.Monitors))
	}

	_, err = ReadClusterInfoFromData(data, "missing")
	if err == nil {
		t.Error("expected error for missing clusterID")
	}

	_, err = ReadClusterInfoFromData([]byte("invalid"), "c1")
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestGetRBDControllerPublishSecretRefFromData(t *testing.T) {
	t.Parallel()
	csiConfig := []cephcsi.ClusterInfo{
		{
			ClusterID: "cluster-1",
			RBD: cephcsi.RBD{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name: "rbd-secret-1", Namespace: "ceph-csi",
				},
			},
		},
	}
	data, err := json.Marshal(csiConfig)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	name, ns, err := GetRBDControllerPublishSecretRefFromData(data, "cluster-1")
	if err != nil {
		t.Fatalf("GetRBDControllerPublishSecretRefFromData() error: %v", err)
	}
	if name != "rbd-secret-1" || ns != "ceph-csi" {
		t.Errorf("got (%q, %q), want (%q, %q)", name, ns, "rbd-secret-1", "ceph-csi")
	}

	_, _, err = GetRBDControllerPublishSecretRefFromData(data, "missing")
	if err == nil {
		t.Error("expected error for missing clusterID")
	}
}

func TestGetCephFSControllerPublishSecretRefFromData(t *testing.T) {
	t.Parallel()
	csiConfig := []cephcsi.ClusterInfo{
		{
			ClusterID: "cluster-1",
			CephFS: cephcsi.CephFS{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name: "cephfs-secret-1", Namespace: "ceph-csi",
				},
			},
		},
	}
	data, err := json.Marshal(csiConfig)
	if err != nil {
		t.Fatalf("json.Marshal() error: %v", err)
	}

	name, ns, err := GetCephFSControllerPublishSecretRefFromData(data, "cluster-1")
	if err != nil {
		t.Fatalf("GetCephFSControllerPublishSecretRefFromData() error: %v", err)
	}
	if name != "cephfs-secret-1" || ns != "ceph-csi" {
		t.Errorf("got (%q, %q), want (%q, %q)", name, ns, "cephfs-secret-1", "ceph-csi")
	}

	_, _, err = GetCephFSControllerPublishSecretRefFromData(data, "missing")
	if err == nil {
		t.Error("expected error for missing clusterID")
	}
}

func TestGetRBDControllerPublishSecretRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		clusterID string
		want      corev1.SecretReference
	}{
		{
			name:      "get secret in cluster-1",
			clusterID: "cluster-1",
			want: corev1.SecretReference{
				Name:      "rbd-secret-1",
				Namespace: "ceph-csi",
			},
		},
		{
			name:      "get secret in cluster-2",
			clusterID: "cluster-2",
			want: corev1.SecretReference{
				Name:      "rbd-secret-2",
				Namespace: "ceph-csi",
			},
		},
		{
			name:      "get secret where not provided in cluster-5",
			clusterID: "cluster-5",
			want:      corev1.SecretReference{Name: "", Namespace: ""},
		},
	}

	csiConfig := []cephcsi.ClusterInfo{
		{
			ClusterID: "cluster-1",
			RBD: cephcsi.RBD{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name:      "rbd-secret-1",
					Namespace: "ceph-csi",
				},
			},
		},
		{
			ClusterID: "cluster-2",
			RBD: cephcsi.RBD{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name:      "rbd-secret-2",
					Namespace: "ceph-csi",
				},
			},
		},
		{
			ClusterID: "cluster-3",
			RBD: cephcsi.RBD{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name:      "",
					Namespace: "ceph-csi",
				},
			},
		},
		{
			ClusterID: "cluster-4",
			RBD: cephcsi.RBD{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name:      "rbd-secret-4",
					Namespace: "",
				},
			},
		},
		{
			ClusterID: "cluster-5",
			RBD:       cephcsi.RBD{},
		},
	}
	csiConfigFileContent, err := json.Marshal(csiConfig)
	if err != nil {
		t.Errorf("failed to marshal csi config info %v", err)
	}
	tmpConfPath := t.TempDir() + "/ceph-csi.json"
	err = os.WriteFile(tmpConfPath, csiConfigFileContent, 0o600)
	if err != nil {
		t.Errorf("failed to write %s file content: %v", CsiConfigFile, err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			secretName, secretNamespace, err := GetRBDControllerPublishSecretRef(tmpConfPath, tt.clusterID)
			if err != nil {
				t.Errorf("GetRBDControllerPublishSecretRef() error = %v", err)

				return
			}
			if tt.want.Name != secretName || tt.want.Namespace != secretNamespace {
				t.Errorf("GetRBDControllerPublishSecretRef() = (%v, %v), want (%v, %v)",
					secretName, secretNamespace, tt.want.Name, tt.want.Namespace)
			}
		})
	}
}

func TestGetCephFSControllerPublishSecretRef(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		clusterID string
		want      corev1.SecretReference
	}{
		{
			name:      "get secret in cluster-1",
			clusterID: "cluster-1",
			want: corev1.SecretReference{
				Name:      "cephfs-secret-1",
				Namespace: "ceph-csi",
			},
		},
		{
			name:      "get secret in cluster-2",
			clusterID: "cluster-2",
			want: corev1.SecretReference{
				Name:      "cephfs-secret-2",
				Namespace: "ceph-csi",
			},
		},
		{
			name:      "get secret where not provided in cluster-5",
			clusterID: "cluster-5",
			want:      corev1.SecretReference{Name: "", Namespace: ""},
		},
	}

	csiConfig := []cephcsi.ClusterInfo{
		{
			ClusterID: "cluster-1",
			CephFS: cephcsi.CephFS{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name:      "cephfs-secret-1",
					Namespace: "ceph-csi",
				},
			},
		},
		{
			ClusterID: "cluster-2",
			CephFS: cephcsi.CephFS{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name:      "cephfs-secret-2",
					Namespace: "ceph-csi",
				},
			},
		},
		{
			ClusterID: "cluster-3",
			CephFS: cephcsi.CephFS{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name:      "",
					Namespace: "ceph-csi",
				},
			},
		},
		{
			ClusterID: "cluster-4",
			CephFS: cephcsi.CephFS{
				ControllerPublishSecretRef: corev1.SecretReference{
					Name:      "cephfs-secret-4",
					Namespace: "",
				},
			},
		},
		{
			ClusterID: "cluster-5",
			CephFS:    cephcsi.CephFS{},
		},
	}
	csiConfigFileContent, err := json.Marshal(csiConfig)
	if err != nil {
		t.Errorf("failed to marshal csi config info %v", err)
	}
	tmpConfPath := t.TempDir() + "/ceph-csi.json"
	err = os.WriteFile(tmpConfPath, csiConfigFileContent, 0o600)
	if err != nil {
		t.Errorf("failed to write %s file content: %v", CsiConfigFile, err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			secretName, secretNamespace, err := GetCephFSControllerPublishSecretRef(tmpConfPath, tt.clusterID)
			if err != nil {
				t.Errorf("GetCephFSControllerPublishSecretRef() error = %v", err)

				return
			}
			if tt.want.Name != secretName || tt.want.Namespace != secretNamespace {
				t.Errorf("GetCephFSControllerPublishSecretRef() = (%v, %v), want (%v, %v)",
					secretName, secretNamespace, tt.want.Name, tt.want.Namespace)
			}
		})
	}
}
