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

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/go-logr/logr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

func TestMoverName(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	if got := m.Name(); got != "ceph" {
		t.Errorf("Name() = %q, want %q", got, "ceph")
	}
}

func TestInitCached_Source_CephFS(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)

	if m.direction != "src" {
		t.Errorf("direction = %q, want %q", m.direction, "src")
	}
	wantPrefix := cephVolsyncPrefix + "cephfs-"
	if m.namePrefix != wantPrefix {
		t.Errorf("namePrefix = %q, want %q", m.namePrefix, wantPrefix)
	}
	if m.containerName != containerNameCephFS {
		t.Errorf("containerName = %q, want %q", m.containerName, containerNameCephFS)
	}
	if m.serviceSelector["app.kubernetes.io/component"] != containerNameCephFS {
		t.Errorf("serviceSelector component = %q, want %q",
			m.serviceSelector["app.kubernetes.io/component"], containerNameCephFS)
	}
	if m.serviceSelector["app.kubernetes.io/name"] != "src-test-rs" {
		t.Errorf("serviceSelector name = %q, want %q",
			m.serviceSelector["app.kubernetes.io/name"], "src-test-rs")
	}
	if m.serviceSelector["app.kubernetes.io/part-of"] != "ceph-volsync-plugin" {
		t.Errorf("serviceSelector part-of = %q, want %q",
			m.serviceSelector["app.kubernetes.io/part-of"], "ceph-volsync-plugin")
	}
}

func TestInitCached_Dest_RBD(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverRBD)

	if m.direction != "dst" {
		t.Errorf("direction = %q, want %q", m.direction, "dst")
	}
	wantPrefix := cephVolsyncPrefix + "rbd-"
	if m.namePrefix != wantPrefix {
		t.Errorf("namePrefix = %q, want %q", m.namePrefix, wantPrefix)
	}
	if m.containerName != containerNameRBD {
		t.Errorf("containerName = %q, want %q", m.containerName, containerNameRBD)
	}
	if m.serviceSelector["app.kubernetes.io/name"] != "dst-test-rs" {
		t.Errorf("serviceSelector name = %q, want %q",
			m.serviceSelector["app.kubernetes.io/name"], "dst-test-rs")
	}
}

func TestInitCached_DestPVC_Provided(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverCephFS)

	// newTestMover sets mainPVCName to "test-pvc"
	if !m.destPVCIsProvided {
		t.Error("destPVCIsProvided = false, want true")
	}
	if m.destPVCName != "test-pvc" {
		t.Errorf("destPVCName = %q, want %q", m.destPVCName, "test-pvc")
	}
}

func TestInitCached_DestPVC_Generated(t *testing.T) {
	t.Parallel()

	scheme := newTestScheme()
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		Build()

	owner := newTestOwner("my-rd", "test-ns")
	m := &Mover{
		client:            fakeClient,
		logger:            logr.Discard(),
		eventRecorder:     &fakeEventRecorder{},
		owner:             owner,
		isSource:          false,
		moverType:         constant.MoverRBD,
		mainPVCName:       nil, // not provided
		sourceStatus:      &volsyncv1alpha1.ReplicationSourceRsyncTLSStatus{},
		destStatus:        &volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus{},
		latestMoverStatus: &volsyncv1alpha1.MoverStatus{},
		options:           map[string]string{},
	}
	m.initCached()

	if m.destPVCIsProvided {
		t.Error("destPVCIsProvided = true, want false")
	}
	wantName := cephVolsyncPrefix + "my-rd-dst"
	if m.destPVCName != wantName {
		t.Errorf("destPVCName = %q, want %q", m.destPVCName, wantName)
	}
}
