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
	"context"

	"github.com/backube/volsync/controllers/utils"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

type rsyncSvcDescription struct {
	Context     context.Context
	Client      client.Client
	Service     *corev1.Service
	Owner       metav1.Object
	Type        *corev1.ServiceType
	Selector    map[string]string
	Port        *int32
	Annotations map[string]string
	MoverType   MoverType
}

func (d *rsyncSvcDescription) Reconcile(l logr.Logger) error {
	logger := l.WithValues("service", client.ObjectKeyFromObject(d.Service))

	op, err := ctrlutil.CreateOrUpdate(d.Context, d.Client, d.Service, func() error {
		if err := ctrl.SetControllerReference(d.Owner, d.Service, d.Client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.SetOwnedByVolSync(d.Service)

		if d.Service.ObjectMeta.Annotations == nil {
			d.Service.ObjectMeta.Annotations = map[string]string{}
		}
		updateAnnotationsOrDefault(d.Service.ObjectMeta.Annotations, d.Annotations)

		if d.Type != nil {
			d.Service.Spec.Type = *d.Type
		} else {
			d.Service.Spec.Type = corev1.ServiceTypeClusterIP
		}
		d.Service.Spec.Selector = d.Selector
		if len(d.Service.Spec.Ports) != 2 {
			d.Service.Spec.Ports = []corev1.ServicePort{{}, {}}
		}

		if d.MoverType == MoverTypeRBD {
			d.Service.Spec.Ports[0].Name = "rbd-mover"
		} else {
			d.Service.Spec.Ports[0].Name = "cephfs-mover"
		}
		if d.Port != nil {
			d.Service.Spec.Ports[0].Port = *d.Port
		} else {
			d.Service.Spec.Ports[0].Port = 8000
		}
		d.Service.Spec.Ports[0].Protocol = corev1.ProtocolTCP
		d.Service.Spec.Ports[0].TargetPort = intstr.FromInt32(tlsContainerPort)
		if d.Service.Spec.Type == corev1.ServiceTypeClusterIP {
			d.Service.Spec.Ports[0].NodePort = 0
		}

		if d.MoverType == MoverTypeRBD {
			d.Service.Spec.Ports[1].Name = "rbd-grpc-server"
			d.Service.Spec.Ports[1].Port = 8080
			d.Service.Spec.Ports[1].Protocol = corev1.ProtocolTCP
			d.Service.Spec.Ports[1].TargetPort = intstr.FromInt32(8080)
		} else {
			d.Service.Spec.Ports[1].Name = "rsync-server"
			d.Service.Spec.Ports[1].Port = 8873
			d.Service.Spec.Ports[1].Protocol = corev1.ProtocolTCP
			d.Service.Spec.Ports[1].TargetPort = intstr.FromInt32(8873)
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

func updateAnnotationsOrDefault(annotations, userSuppliedAnnotations map[string]string) {
	if userSuppliedAnnotations == nil {
		// Set our default annotations
		annotations["service.beta.kubernetes.io/aws-load-balancer-type"] = "nlb"
	} else {
		// Use user-supplied annotations - do not replace Annotations entirely in case of system-added annotations
		updateMap(annotations, userSuppliedAnnotations)
	}
}

// Update map1 with any k,v pairs from map2
func updateMap(map1, map2 map[string]string) {
	for k, v := range map2 {
		map1[k] = v
	}
}
