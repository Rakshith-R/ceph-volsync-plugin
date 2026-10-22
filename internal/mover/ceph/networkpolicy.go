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
	"context"

	"github.com/backube/volsync/controllers/utils"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

// ensureNetworkPolicy creates or updates the NetworkPolicy that limits ingress
// to the destination mover pod to its listening ports. Source movers are
// skipped: they run with hostNetwork, where NetworkPolicy is not enforced.
func (m *Mover) ensureNetworkPolicy(ctx context.Context) error {
	if m.isSource {
		return nil
	}

	np := &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cephVolsyncPrefix + m.direction + "-" + m.owner.GetName(),
			Namespace: m.owner.GetNamespace(),
		},
	}
	op, err := ctrlutil.CreateOrUpdate(ctx, m.client, np, func() error {
		if err := ctrl.SetControllerReference(m.owner, np, m.client.Scheme()); err != nil {
			return err
		}
		utils.SetOwnedByVolSync(np)

		np.Spec.PodSelector = metav1.LabelSelector{MatchLabels: m.serviceSelector}
		np.Spec.Ingress = []networkingv1.NetworkPolicyIngressRule{
			{Ports: destinationPorts(m.moverType)},
		}
		np.Spec.Egress = []networkingv1.NetworkPolicyEgressRule{{}}
		np.Spec.PolicyTypes = []networkingv1.PolicyType{
			networkingv1.PolicyTypeIngress,
			networkingv1.PolicyTypeEgress,
		}
		return nil
	})
	if err != nil {
		m.logger.Error(err, "NetworkPolicy reconcile failed")
		return err
	}

	m.logger.V(1).Info("NetworkPolicy reconciled", "operation", op)
	return nil
}

// destinationPorts returns the TCP ports the destination mover accepts traffic
// on. These match the Service ports built in svcDescription.Reconcile.
func destinationPorts(moverType constant.MoverType) []networkingv1.NetworkPolicyPort {
	ports := []networkingv1.NetworkPolicyPort{
		tcpPort(constant.TLSPort),
		tcpPort(constant.DirectTLSPort),
	}
	if moverType == constant.MoverCephFS {
		ports = append(ports, tcpPort(constant.RsyncStunnelPort))
	}
	return ports
}

// tcpPort returns a NetworkPolicyPort for the given TCP port number.
func tcpPort(port int32) networkingv1.NetworkPolicyPort {
	proto := corev1.ProtocolTCP
	p := intstr.FromInt32(port)
	return networkingv1.NetworkPolicyPort{Protocol: &proto, Port: &p}
}
