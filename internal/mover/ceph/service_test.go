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

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

func TestEnsureService_Source(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	ctx := t.Context()

	cont, err := m.ensureServiceAndPublishAddress(ctx)
	if err != nil {
		t.Fatalf("ensureServiceAndPublishAddress() error: %v", err)
	}
	if !cont {
		t.Error("expected true for source mover (no service needed)")
	}
}

func TestEnsureService_WithAddress(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverCephFS)
	addr := "10.0.0.1"
	m.address = &addr
	ctx := t.Context()

	cont, err := m.ensureServiceAndPublishAddress(ctx)
	if err != nil {
		t.Fatalf("ensureServiceAndPublishAddress() error: %v", err)
	}
	if !cont {
		t.Error("expected true when address is provided")
	}
}

func TestUpdateStatusAddress_Sets(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverCephFS)
	addr := "10.0.0.5"
	m.updateStatusAddress(&addr)

	if m.destStatus.Address == nil || *m.destStatus.Address != addr {
		t.Errorf("destStatus.Address = %v, want %q", m.destStatus.Address, addr)
	}
}

func TestUpdateStatusAddress_Nil(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverCephFS)
	addr := "10.0.0.5"
	m.updateStatusAddress(&addr)
	m.updateStatusAddress(nil)

	if m.destStatus.Address != nil {
		t.Errorf("destStatus.Address = %v, want nil", m.destStatus.Address)
	}
}

func TestReconcile_CephFS(t *testing.T) {
	t.Parallel()
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := t.Context()

	owner := newTestOwner("test-rs", "test-ns")
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-svc",
			Namespace: "test-ns",
		},
	}
	desc := svcDescription{
		Context:   ctx,
		Client:    cl,
		Service:   svc,
		Owner:     owner,
		Selector:  map[string]string{"app": "test"},
		MoverType: constant.MoverCephFS,
	}
	if err := desc.Reconcile(logr.Discard()); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}

	// Verify service was created with correct ports
	created := &corev1.Service{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(svc), created); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if len(created.Spec.Ports) != 2 {
		t.Fatalf("len(ports) = %d, want 2", len(created.Spec.Ports))
	}
	if created.Spec.Ports[0].Port != constant.TLSPort {
		t.Errorf("port[0] = %d, want %d",
			created.Spec.Ports[0].Port, constant.TLSPort)
	}
	if created.Spec.Ports[1].Port != constant.RsyncStunnelPort {
		t.Errorf("port[1] = %d, want %d",
			created.Spec.Ports[1].Port, constant.RsyncStunnelPort)
	}
	if created.Spec.Ports[0].Name != grpcServerPortName {
		t.Errorf("port[0].Name = %q, want %q",
			created.Spec.Ports[0].Name, grpcServerPortName)
	}
}

func TestReconcile_RBD(t *testing.T) {
	t.Parallel()
	scheme := newTestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	ctx := t.Context()

	owner := newTestOwner("test-rs", "test-ns")
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-svc",
			Namespace: "test-ns",
		},
	}
	desc := svcDescription{
		Context:   ctx,
		Client:    cl,
		Service:   svc,
		Owner:     owner,
		Selector:  map[string]string{"app": "test"},
		MoverType: constant.MoverRBD,
	}
	if err := desc.Reconcile(logr.Discard()); err != nil {
		t.Fatalf("Reconcile() error: %v", err)
	}

	created := &corev1.Service{}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(svc), created); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if len(created.Spec.Ports) != 1 {
		t.Fatalf("len(ports) = %d, want 1", len(created.Spec.Ports))
	}
	if created.Spec.Ports[0].Name != grpcServerPortName {
		t.Errorf("port[0].Name = %q, want %q",
			created.Spec.Ports[0].Name, grpcServerPortName)
	}
}
