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

package mover

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

func TestUpdateStatusPSK_Source(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	name := "my-psk-secret"
	m.updateStatusPSK(&name)

	if m.sourceStatus.KeySecret == nil || *m.sourceStatus.KeySecret != name {
		t.Errorf("sourceStatus.KeySecret = %v, want %q", m.sourceStatus.KeySecret, name)
	}
}

func TestUpdateStatusPSK_Dest(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverCephFS)
	name := "my-psk-secret"
	m.updateStatusPSK(&name)

	if m.destStatus.KeySecret == nil || *m.destStatus.KeySecret != name {
		t.Errorf("destStatus.KeySecret = %v, want %q", m.destStatus.KeySecret, name)
	}
}

func TestEnsureSecrets_UserKey(t *testing.T) {
	t.Parallel()
	keyName := "user-key-secret"
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      keyName,
			Namespace: "test-ns",
		},
		Data: map[string][]byte{
			"psk.txt": []byte("volsync:abcdef"),
		},
	}

	m := newTestMover(t, true, constant.MoverCephFS, secret)
	m.key = &keyName
	ctx := t.Context()

	got, err := m.ensureSecrets(ctx)
	if err != nil {
		t.Fatalf("ensureSecrets() error: %v", err)
	}
	if got == nil || *got != keyName {
		t.Errorf("ensureSecrets() = %v, want %q", got, keyName)
	}
}

func TestEnsureSecrets_AutoGen(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	m.key = nil
	ctx := t.Context()

	got, err := m.ensureSecrets(ctx)
	if err != nil {
		t.Fatalf("ensureSecrets() error: %v", err)
	}
	if got == nil {
		t.Fatal("ensureSecrets() returned nil")
	}

	// Verify secret was created
	secret := &corev1.Secret{}
	if err := m.client.Get(ctx, client.ObjectKey{
		Name:      *got,
		Namespace: "test-ns",
	}, secret); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	// Fake client does not convert StringData to Data (server-side behavior),
	// so check StringData directly.
	if _, ok := secret.StringData["psk.txt"]; !ok {
		t.Error("secret missing psk.txt field")
	}
}

func TestFetchCSIConfigData(t *testing.T) {
	t.Parallel()
	srcCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ceph-csi-config",
			Namespace: "rook-ceph",
		},
		Data: map[string]string{
			"config.json":          `[{"clusterID":"test-cluster","monitors":["mon1"]}]`,
			"cluster-mapping.json": `[{"source":"a","dest":"b"}]`,
		},
	}
	m := newTestMover(t, true, constant.MoverCephFS, srcCM)
	ctx := t.Context()

	data, err := m.fetchCSIConfigData(ctx)
	if err != nil {
		t.Fatalf("fetchCSIConfigData() error: %v", err)
	}
	if data["config.json"] != srcCM.Data["config.json"] {
		t.Errorf("config.json = %q, want %q", data["config.json"], srcCM.Data["config.json"])
	}
	if data["cluster-mapping.json"] != srcCM.Data["cluster-mapping.json"] {
		t.Errorf("cluster-mapping.json = %q, want %q", data["cluster-mapping.json"], srcCM.Data["cluster-mapping.json"])
	}
}

func TestFetchCSIConfigData_MissingConfigJSON(t *testing.T) {
	t.Parallel()
	srcCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ceph-csi-config",
			Namespace: "rook-ceph",
		},
		Data: map[string]string{},
	}
	m := newTestMover(t, true, constant.MoverCephFS, srcCM)
	ctx := t.Context()

	_, err := m.fetchCSIConfigData(ctx)
	if err == nil {
		t.Error("expected error for missing config.json")
	}
}

func TestFetchCSIConfigData_MissingSourceCM(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	ctx := t.Context()

	_, err := m.fetchCSIConfigData(ctx)
	if err == nil {
		t.Error("expected error for missing source ConfigMap")
	}
}

func TestEnsureCephCSIConfigMap(t *testing.T) {
	t.Parallel()
	srcData := map[string]string{
		"config.json":          `[{"clusterID":"test-cluster","monitors":["mon1"]}]`,
		"cluster-mapping.json": `[{"source":"a","dest":"b"}]`,
	}
	m := newTestMover(t, true, constant.MoverCephFS)
	ctx := t.Context()

	got, err := m.ensureCephCSIConfigMap(ctx, srcData)
	if err != nil {
		t.Fatalf("ensureCephCSIConfigMap() error: %v", err)
	}
	if got == nil {
		t.Fatal("ensureCephCSIConfigMap() returned nil")
	}

	// Verify the per-RS/RD ConfigMap was created with correct data
	cm := &corev1.ConfigMap{}
	if err := m.client.Get(ctx, client.ObjectKey{Name: *got, Namespace: "test-ns"}, cm); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if cm.Data["config.json"] != srcData["config.json"] {
		t.Errorf("config.json = %q, want %q", cm.Data["config.json"], srcData["config.json"])
	}
	if cm.Data["cluster-mapping.json"] != srcData["cluster-mapping.json"] {
		t.Errorf("cluster-mapping.json = %q, want %q", cm.Data["cluster-mapping.json"], srcData["cluster-mapping.json"])
	}
}

func TestClusterIDFromStorageClass(t *testing.T) {
	t.Parallel()
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "cephfs-sc"},
		Provisioner: "cephfs.csi.ceph.com",
		Parameters:  map[string]string{"clusterID": "cluster-abc"},
	}
	m := newTestMover(t, true, constant.MoverCephFS, sc)
	ctx := t.Context()

	scName := "cephfs-sc"
	got, err := m.clusterIDFromStorageClass(ctx, &scName)
	if err != nil {
		t.Fatalf("clusterIDFromStorageClass() error: %v", err)
	}
	if got != "cluster-abc" {
		t.Errorf("clusterID = %q, want %q", got, "cluster-abc")
	}
}

func TestClusterIDFromStorageClass_MissingParam(t *testing.T) {
	t.Parallel()
	sc := &storagev1.StorageClass{
		ObjectMeta:  metav1.ObjectMeta{Name: "no-cluster-sc"},
		Provisioner: "cephfs.csi.ceph.com",
		Parameters:  map[string]string{},
	}
	m := newTestMover(t, true, constant.MoverCephFS, sc)
	ctx := t.Context()

	scName := "no-cluster-sc"
	_, err := m.clusterIDFromStorageClass(ctx, &scName)
	if err == nil {
		t.Error("expected error for missing clusterID param")
	}
}

func TestClusterIDFromStorageClass_NilName(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	ctx := t.Context()

	_, err := m.clusterIDFromStorageClass(ctx, nil)
	if err == nil {
		t.Error("expected error for nil storageClassName")
	}
}
