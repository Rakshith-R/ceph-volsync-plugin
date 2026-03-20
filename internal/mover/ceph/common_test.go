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

package mover

import (
	"testing"

	wcommon "github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

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
		MoverType: MoverTypeCephFS,
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
	if created.Spec.Ports[0].Port != wcommon.TLSPort {
		t.Errorf("port[0] = %d, want %d", created.Spec.Ports[0].Port, wcommon.TLSPort)
	}
	if created.Spec.Ports[1].Port != wcommon.RsyncStunnelPort {
		t.Errorf("port[1] = %d, want %d", created.Spec.Ports[1].Port, wcommon.RsyncStunnelPort)
	}
	if created.Spec.Ports[0].Name != grpcServerPortName {
		t.Errorf("port[0].Name = %q, want %q", created.Spec.Ports[0].Name, grpcServerPortName)
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
		MoverType: MoverTypeRBD,
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
		t.Errorf("port[0].Name = %q, want %q", created.Spec.Ports[0].Name, grpcServerPortName)
	}
}
