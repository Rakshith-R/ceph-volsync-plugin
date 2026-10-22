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
	"reflect"
	"testing"

	"github.com/backube/volsync/controllers/utils"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

// getTestNetworkPolicy fetches the destination NetworkPolicy for testNamespace/test-rs.
func getTestNetworkPolicy(t *testing.T, m *Mover) *networkingv1.NetworkPolicy {
	t.Helper()

	np := &networkingv1.NetworkPolicy{}
	key := client.ObjectKey{Name: "ceph-volsync-dst-test-rs", Namespace: testNamespace}
	if err := m.client.Get(t.Context(), key, np); err != nil {
		t.Fatalf("Get() NetworkPolicy error: %v", err)
	}
	return np
}

// wantPorts checks that got is exactly the want TCP ports, in order.
func wantPorts(t *testing.T, got []networkingv1.NetworkPolicyPort, want ...int32) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(ports) = %d, want %d", len(got), len(want))
	}
	for i, p := range got {
		if p.Protocol == nil || *p.Protocol != corev1.ProtocolTCP {
			t.Errorf("port[%d].Protocol = %v, want TCP", i, p.Protocol)
		}
		if p.Port == nil || p.Port.IntValue() != int(want[i]) {
			t.Errorf("port[%d].Port = %v, want %d", i, p.Port, want[i])
		}
	}
}

func TestEnsureNetworkPolicy_SourceSkipped(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	ctx := t.Context()

	if err := m.ensureNetworkPolicy(ctx); err != nil {
		t.Fatalf("ensureNetworkPolicy() error: %v", err)
	}

	nps := &networkingv1.NetworkPolicyList{}
	if err := m.client.List(ctx, nps); err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(nps.Items) != 0 {
		t.Errorf("len(NetworkPolicies) = %d, want 0 for source mover", len(nps.Items))
	}
}

func TestEnsureNetworkPolicy_RBD(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverRBD)

	if err := m.ensureNetworkPolicy(t.Context()); err != nil {
		t.Fatalf("ensureNetworkPolicy() error: %v", err)
	}
	np := getTestNetworkPolicy(t, m)

	if len(np.Spec.Ingress) != 1 {
		t.Fatalf("len(Ingress) = %d, want 1", len(np.Spec.Ingress))
	}
	wantPorts(t, np.Spec.Ingress[0].Ports,
		constant.TLSPort, constant.DirectTLSPort)

	if !reflect.DeepEqual(np.Spec.PodSelector.MatchLabels, m.serviceSelector) {
		t.Errorf("PodSelector = %v, want %v",
			np.Spec.PodSelector.MatchLabels, m.serviceSelector)
	}
	if len(np.Spec.Egress) != 1 || len(np.Spec.Egress[0].Ports) != 0 ||
		len(np.Spec.Egress[0].To) != 0 {
		t.Errorf("Egress = %v, want one open rule", np.Spec.Egress)
	}
	wantTypes := []networkingv1.PolicyType{
		networkingv1.PolicyTypeIngress,
		networkingv1.PolicyTypeEgress,
	}
	if !reflect.DeepEqual(np.Spec.PolicyTypes, wantTypes) {
		t.Errorf("PolicyTypes = %v, want %v", np.Spec.PolicyTypes, wantTypes)
	}

	ref := metav1.GetControllerOf(np)
	if ref == nil || !*ref.Controller || ref.UID != m.owner.GetUID() {
		t.Errorf("controller ownerRef = %v, want controller ref to owner", ref)
	}
	if !utils.IsOwnedByVolsync(np) {
		t.Error("NetworkPolicy missing VolSync ownership label")
	}
	for k, v := range np.Labels {
		if v == string(m.owner.GetUID()) {
			t.Errorf("label %q carries cleanup mark, want none", k)
		}
	}
}

func TestEnsureNetworkPolicy_CephFS(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverCephFS)

	if err := m.ensureNetworkPolicy(t.Context()); err != nil {
		t.Fatalf("ensureNetworkPolicy() error: %v", err)
	}
	np := getTestNetworkPolicy(t, m)

	if len(np.Spec.Ingress) != 1 {
		t.Fatalf("len(Ingress) = %d, want 1", len(np.Spec.Ingress))
	}
	wantPorts(t, np.Spec.Ingress[0].Ports,
		constant.TLSPort, constant.DirectTLSPort, constant.RsyncStunnelPort)
}

func TestEnsureNetworkPolicy_SwitchToRBDReplacesPorts(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, constant.MoverCephFS)
	ctx := t.Context()

	if err := m.ensureNetworkPolicy(ctx); err != nil {
		t.Fatalf("ensureNetworkPolicy() CephFS error: %v", err)
	}

	m.moverType = constant.MoverRBD
	if err := m.ensureNetworkPolicy(ctx); err != nil {
		t.Fatalf("ensureNetworkPolicy() RBD error: %v", err)
	}
	np := getTestNetworkPolicy(t, m)

	if len(np.Spec.Ingress) != 1 {
		t.Fatalf("len(Ingress) = %d, want 1", len(np.Spec.Ingress))
	}
	wantPorts(t, np.Spec.Ingress[0].Ports,
		constant.TLSPort, constant.DirectTLSPort)
}
