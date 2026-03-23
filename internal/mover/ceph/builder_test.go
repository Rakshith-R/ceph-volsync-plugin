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
	"flag"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/spf13/viper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

func newTestBuilder(t *testing.T) *Builder {
	t.Helper()
	v := viper.New()
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	b, err := newBuilder(v, flags)
	if err != nil {
		t.Fatalf("newBuilder() error: %v", err)
	}
	return b
}

func TestBuilderName(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	if got := b.Name(); got != "ceph" {
		t.Errorf("Name() = %q, want %q", got, "ceph")
	}
}

func TestVersionInfo(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	info := b.VersionInfo()
	if info == "" {
		t.Error("VersionInfo() returned empty string")
	}
	if got := b.getCephContainerImage(); got != defaultCephContainerImage {
		t.Errorf("getCephContainerImage() = %q, want %q", got, defaultCephContainerImage)
	}
}

func TestNewBuilder(t *testing.T) {
	t.Parallel()
	v := viper.New()
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	b, err := newBuilder(v, flags)
	if err != nil {
		t.Fatalf("newBuilder() error: %v", err)
	}
	if b.viper != v {
		t.Error("viper not set correctly")
	}
	// Check default was set
	if got := v.GetString(cephContainerImageFlag); got != defaultCephContainerImage {
		t.Errorf("default image = %q, want %q", got, defaultCephContainerImage)
	}
}

func TestFromSource_NilExternal(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	src := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: "ns"},
		Spec:       volsyncv1alpha1.ReplicationSourceSpec{},
	}
	m, err := b.FromSource(cl, logr.Discard(), &fakeEventRecorder{}, src, false)
	if err != nil {
		t.Fatalf("FromSource() error: %v", err)
	}
	if m != nil {
		t.Error("expected nil mover for nil External")
	}
}

func TestFromSource_UnknownProvider(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	src := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: "ns"},
		Spec: volsyncv1alpha1.ReplicationSourceSpec{
			External: &volsyncv1alpha1.ReplicationSourceExternalSpec{
				Provider: "unknown.csi.driver",
			},
		},
	}
	m, err := b.FromSource(cl, logr.Discard(), &fakeEventRecorder{}, src, false)
	if err != nil {
		t.Fatalf("FromSource() error: %v", err)
	}
	if m != nil {
		t.Error("expected nil mover for unknown provider")
	}
}

func TestFromSource_MissingParams(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	src := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: "ns"},
		Spec: volsyncv1alpha1.ReplicationSourceSpec{
			External: &volsyncv1alpha1.ReplicationSourceExternalSpec{
				Provider: "cephfs.csi.ceph.com",
				// No parameters
			},
		},
		Status: &volsyncv1alpha1.ReplicationSourceStatus{},
	}
	_, err := b.FromSource(cl, logr.Discard(), &fakeEventRecorder{}, src, false)
	if err == nil {
		t.Error("expected error for missing parameters")
	}
}

func TestFromSource_CephFS(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	src := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: "ns"},
		Spec: volsyncv1alpha1.ReplicationSourceSpec{
			SourcePVC: "my-pvc",
			External: &volsyncv1alpha1.ReplicationSourceExternalSpec{
				Provider:   "cephfs.csi.ceph.com",
				Parameters: map[string]string{"storageClassName": "cephfs-sc"},
			},
		},
		Status: &volsyncv1alpha1.ReplicationSourceStatus{},
	}
	m, err := b.FromSource(cl, logr.Discard(), &fakeEventRecorder{}, src, false)
	if err != nil {
		t.Fatalf("FromSource() error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil mover")
	}
	cephMover, ok := m.(*Mover)
	if !ok {
		t.Fatal("expected *Mover type")
	}
	if cephMover.moverType != constant.MoverCephFS {
		t.Errorf("moverType = %q, want %q", cephMover.moverType, constant.MoverCephFS)
	}
	if !cephMover.isSource {
		t.Error("isSource = false, want true")
	}
}

func TestFromSource_RBD(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	src := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rs", Namespace: "ns"},
		Spec: volsyncv1alpha1.ReplicationSourceSpec{
			SourcePVC: "my-pvc",
			External: &volsyncv1alpha1.ReplicationSourceExternalSpec{
				Provider:   "rbd.csi.ceph.com",
				Parameters: map[string]string{"storageClassName": "rbd-sc"},
			},
		},
		Status: &volsyncv1alpha1.ReplicationSourceStatus{},
	}
	m, err := b.FromSource(cl, logr.Discard(), &fakeEventRecorder{}, src, false)
	if err != nil {
		t.Fatalf("FromSource() error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil mover")
	}
	cephMover := m.(*Mover)
	if cephMover.moverType != constant.MoverRBD {
		t.Errorf("moverType = %q, want %q", cephMover.moverType, constant.MoverRBD)
	}
}

func TestFromDestination_NilExternal(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	logger := logr.Discard()

	dst := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "rd", Namespace: "ns"},
		Spec:       volsyncv1alpha1.ReplicationDestinationSpec{},
	}
	m, err := b.FromDestination(cl, logger, &fakeEventRecorder{}, dst, false)
	if err != nil {
		t.Fatalf("FromDestination() error: %v", err)
	}
	if m != nil {
		t.Error("expected nil mover for nil External")
	}
}

func TestFromDestination_CephFS(t *testing.T) {
	t.Parallel()
	b := newTestBuilder(t)
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	logger := logr.Discard()

	dst := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "rd", Namespace: "ns"},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			External: &volsyncv1alpha1.ReplicationDestinationExternalSpec{
				Provider:   "nfs.csi.ceph.com",
				Parameters: map[string]string{"storageClassName": "nfs-sc"},
			},
		},
		Status: &volsyncv1alpha1.ReplicationDestinationStatus{},
	}
	m, err := b.FromDestination(cl, logger, &fakeEventRecorder{}, dst, false)
	if err != nil {
		t.Fatalf("FromDestination() error: %v", err)
	}
	if m == nil {
		t.Fatal("expected non-nil mover")
	}
	cephMover := m.(*Mover)
	if cephMover.moverType != constant.MoverCephFS {
		t.Errorf("moverType = %q, want %q", cephMover.moverType, constant.MoverCephFS)
	}
	if cephMover.isSource {
		t.Error("isSource = true, want false")
	}
}
