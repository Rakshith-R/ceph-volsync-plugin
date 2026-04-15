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
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

// fakeEventRecorder implements events.EventRecorder with no-ops.
type fakeEventRecorder struct{}

func (f *fakeEventRecorder) Eventf(
	_ runtime.Object, _ runtime.Object,
	_, _, _, _ string, _ ...interface{},
) {
}

// newTestScheme registers all schemes needed by the ceph mover package.
func newTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(volsyncv1alpha1.AddToScheme(s))
	utilruntime.Must(snapv1.AddToScheme(s))
	utilruntime.Must(storagev1.AddToScheme(s))
	return s
}

// newTestMover builds a Mover with a fake client for white-box unit testing.
// Pass pre-existing k8s objects via objs to seed the fake client.
func newTestMover(
	t *testing.T,
	isSource bool,
	moverType constant.MoverType,
	objs ...client.Object,
) *Mover {
	t.Helper()

	scheme := newTestScheme()
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		Build()

	owner := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rs",
			Namespace: "test-ns",
			UID:       "test-uid",
		},
	}

	pvcName := "test-pvc"
	m := &Mover{
		client:             fakeClient,
		logger:             logr.Discard(),
		eventRecorder:      &fakeEventRecorder{},
		owner:              owner,
		isSource:           isSource,
		moverType:          moverType,
		mainPVCName:        &pvcName,
		sourceStatus:       &volsyncv1alpha1.ReplicationSourceRsyncTLSStatus{},
		destStatus:         &volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus{},
		latestMoverStatus:  &volsyncv1alpha1.MoverStatus{},
		options:            map[string]string{},
		csiConfigName:      "ceph-csi-config",
		csiConfigNamespace: "rook-ceph",
	}
	m.initCached()

	return m
}

// newTestOwner creates a ReplicationSource for use as an owner in tests.
func newTestOwner(name, namespace string) *volsyncv1alpha1.ReplicationSource {
	return &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       "test-uid",
		},
	}
}

// ptrTo returns a pointer to the given value.
func ptrTo[T any](v T) *T {
	return &v
}

// newSnapshot creates a ready VolumeSnapshot in
// "test-ns" with the given name and labels.
func newSnapshot(
	name string,
	labels map[string]string,
) *snapv1.VolumeSnapshot {
	snap := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "test-ns",
			Labels:    labels,
		},
		Status: &snapv1.VolumeSnapshotStatus{
			ReadyToUse: ptrTo(true),
		},
	}
	return snap
}
