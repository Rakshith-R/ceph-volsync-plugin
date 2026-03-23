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
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

const grpcServerPortName = "grpc-server"

// svcDescription holds parameters for reconciling
// the mover Service (TLS port + optional rsync port).
type svcDescription struct {
	Context     context.Context
	Client      client.Client
	Service     *corev1.Service
	Owner       metav1.Object
	Type        *corev1.ServiceType
	Selector    map[string]string
	Port        *int32
	Annotations map[string]string
	MoverType   constant.MoverType
}

// Reconcile creates or updates the Service with TLS and protocol-specific ports.
func (d *svcDescription) Reconcile(l logr.Logger) error {
	logger := l.WithValues("service", client.ObjectKeyFromObject(d.Service))

	op, err := ctrlutil.CreateOrUpdate(d.Context, d.Client, d.Service, func() error {
		if err := ctrl.SetControllerReference(d.Owner, d.Service, d.Client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.SetOwnedByVolSync(d.Service)

		d.Service.Spec.Type = corev1.ServiceTypeClusterIP
		d.Service.Spec.Selector = d.Selector

		portCount := 1
		if d.MoverType == constant.MoverCephFS {
			portCount = 2
		}
		if len(d.Service.Spec.Ports) != portCount {
			d.Service.Spec.Ports = make([]corev1.ServicePort, portCount)
		}

		d.Service.Spec.Ports[0].Name = grpcServerPortName
		d.Service.Spec.Ports[0].Port = constant.TLSPort
		d.Service.Spec.Ports[0].Protocol = corev1.ProtocolTCP
		d.Service.Spec.Ports[0].TargetPort = intstr.FromInt32(constant.TLSPort)
		d.Service.Spec.Ports[0].NodePort = 0

		if d.MoverType == constant.MoverCephFS {
			d.Service.Spec.Ports[1].Name = "rsync-server"
			d.Service.Spec.Ports[1].Port = constant.RsyncStunnelPort
			d.Service.Spec.Ports[1].Protocol = corev1.ProtocolTCP
			d.Service.Spec.Ports[1].TargetPort = intstr.FromInt32(constant.RsyncStunnelPort)
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "Service reconcile failed")
		return err
	}

	logger.V(1).Info("Service reconciled", "operation", op)
	return nil
}

// ensureServiceAndPublishAddress creates a Service for destination movers
// without an explicit address, then publishes the assigned address in status.
func (m *Mover) ensureServiceAndPublishAddress(ctx context.Context) (bool, error) {
	if m.address != nil || m.isSource {
		// Connection will be outbound. Don't need a Service
		return true, nil
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.namePrefix + m.direction + "-" + m.owner.GetName(),
			Namespace: m.owner.GetNamespace(),
		},
	}
	svcDesc := svcDescription{
		Context:     ctx,
		Client:      m.client,
		Service:     service,
		Owner:       m.owner,
		Type:        m.serviceType,
		Selector:    m.serviceSelector,
		Port:        m.port,
		Annotations: m.serviceAnnotations,
		MoverType:   m.moverType,
	}
	err := svcDesc.Reconcile(m.logger)
	if err != nil {
		return false, err
	}

	return m.publishSvcAddress(service)
}

// publishSvcAddress extracts the Service address and updates the mover status.
// Returns false if the address is not yet available.
func (m *Mover) publishSvcAddress(service *corev1.Service) (bool, error) {
	address := utils.GetServiceAddress(service)
	if address == "" {
		// We don't have an address yet, try again later
		m.updateStatusAddress(nil)
		if service.CreationTimestamp.Add(mover.ServiceAddressTimeout).Before(time.Now()) {
			m.eventRecorder.Eventf(m.owner, service, corev1.EventTypeWarning,
				volsyncv1alpha1.EvRSvcNoAddress, volsyncv1alpha1.EvANone,
				"waiting for an address to be assigned to %s; ensure the proper serviceType was specified",
				utils.KindAndName(m.client.Scheme(), service))
		}
		return false, nil
	}
	m.updateStatusAddress(&address)

	m.logger.V(1).Info("Service addr published", "address", address)
	return true, nil
}

// updateStatusAddress sets the destination status address and emits
// an event when the address changes.
func (m *Mover) updateStatusAddress(address *string) {
	publishEvent := false
	if !m.isSource {
		if m.destStatus.Address == nil ||
			(address != nil && *m.destStatus.Address != *address) {
			publishEvent = true
		}
		m.destStatus.Address = address
	}
	if publishEvent && address != nil {
		m.eventRecorder.Eventf(m.owner, nil, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRSvcAddress, volsyncv1alpha1.EvANone,
			"listening on address %s for incoming connections", *address)
	}
}
