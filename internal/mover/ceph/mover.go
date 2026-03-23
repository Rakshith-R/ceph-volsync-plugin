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

	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/backube/volsync/controllers/volumehandler"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

const (
	dataVolumeName = "data"

	cephVolsyncPrefix = "ceph-volsync-"

	containerNameRBD    = "rbd-mover"
	containerNameCephFS = "cephfs-mover"

	// Paths for ceph-csi config mounted in the operator
	csiConfigMountPath = "/etc/ceph-csi-config"

	// Volume name for ceph-csi secret
	csiSecretVolumeName = "ceph-csi-secret" //nolint:gosec // G101: volume name, not credentials
)

// Mover is the reconciliation logic for the CephFS-based data mover.
type Mover struct {
	// client is the controller-runtime Kubernetes client.
	client client.Client
	// logger is the structured logger for this mover instance.
	logger logr.Logger
	// eventRecorder emits Kubernetes events on the owner CR.
	eventRecorder events.EventRecorder
	// owner is the ReplicationSource or ReplicationDestination CR.
	owner client.Object
	// vh handles PVC and VolumeSnapshot operations.
	vh *volumehandler.VolumeHandler
	// saHandler manages the ServiceAccount for mover Jobs.
	saHandler utils.SAHandler
	// containerImage is the mover worker container image.
	containerImage string
	// moverType selects the mover backend (cephfs or rbd).
	moverType constant.MoverType
	// key is the name of the PSK secret for TLS authentication.
	key *string
	// serviceType overrides the default Service type.
	serviceType *corev1.ServiceType
	// serviceAnnotations are extra annotations for the Service.
	serviceAnnotations map[string]string
	// address is the destination endpoint for source movers.
	address *string
	// port overrides the default stunnel TLS port.
	port *int32
	// isSource is true for source movers, false for destination.
	isSource bool
	// paused indicates the CR has paused replication.
	paused bool
	// mainPVCName is the user-provided PVC name, nil if auto-generated.
	mainPVCName *string
	// privileged enables privileged container security context.
	privileged bool
	// latestMoverStatus tracks the last mover sync result.
	latestMoverStatus *volsyncv1alpha1.MoverStatus
	// moverConfig holds additional mover configuration.
	moverConfig volsyncv1alpha1.MoverConfig
	// sourceStatus is the RsyncTLS status for source CRs.
	sourceStatus *volsyncv1alpha1.ReplicationSourceRsyncTLSStatus
	// destStatus is the RsyncTLS status for destination CRs.
	destStatus *volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus
	// cleanupTempPVC indicates whether to delete the temp PVC on cleanup.
	cleanupTempPVC bool
	// options are the external parameters from the CR spec.
	options map[string]string

	// Precomputed values derived from immutable fields.
	// Set once via initCached() after construction.

	// direction is "src" for source movers, "dst" for destination.
	direction string
	// namePrefix is the K8s resource name prefix, e.g. "volsync-rbd-" or "volsync-cephfs-".
	namePrefix string
	// containerName is the mover container name: "rbd-mover" or "cephfs-mover".
	containerName string
	// serviceSelector is the label selector used for the mover Service and Job pods.
	serviceSelector map[string]string
	// snapStatusLabelKey is the label key used to track snapshot lifecycle status.
	snapStatusLabelKey string
	// destPVCIsProvided indicates whether the destination PVC was explicitly specified.
	destPVCIsProvided bool
	// destPVCName is the destination PVC name (user-provided or auto-generated).
	destPVCName string
}

var _ mover.Mover = &Mover{}

// All object types that are temporary/per-iteration should be listed here. The
// individual objects to be cleaned up must also be marked.
var cleanupTypes = []client.Object{
	&corev1.PersistentVolumeClaim{},
	&snapv1.VolumeSnapshot{},
	&batchv1.Job{},
}

// Name returns the mover's registered name ("ceph").
func (m *Mover) Name() string { return cephMoverName }

// initCached computes derived fields from immutable
// Mover properties. Must be called once after construction.
func (m *Mover) initCached() {
	if m.isSource {
		m.direction = "src"
	} else {
		m.direction = "dst"
	}
	if m.moverType == constant.MoverRBD {
		m.namePrefix = cephVolsyncPrefix + "rbd-"
		m.containerName = containerNameRBD
	} else {
		m.namePrefix = cephVolsyncPrefix + "cephfs-"
		m.containerName = containerNameCephFS
	}
	m.serviceSelector = map[string]string{
		"app.kubernetes.io/name":      m.direction + "-" + m.owner.GetName(),
		"app.kubernetes.io/component": m.containerName,
		"app.kubernetes.io/part-of":   "ceph-volsync-plugin",
	}
	m.snapStatusLabelKey = "ceph.volsync.plugin" +
		"/snapshot-status-" + m.owner.GetName()
	if m.mainPVCName == nil {
		m.destPVCIsProvided = false
		m.destPVCName = cephVolsyncPrefix +
			m.owner.GetName() + "-" + m.direction
	} else {
		m.destPVCIsProvided = true
		m.destPVCName = *m.mainPVCName
	}
}

// Synchronize runs one reconciliation iteration: ensures PVC, Service,
// Secrets, ServiceAccount, CSI config, and Job, returning Complete on success.
func (m *Mover) Synchronize(ctx context.Context) (mover.Result, error) {
	var err error

	// Allocate temporary data PVC
	var dataPVC *corev1.PersistentVolumeClaim
	if m.isSource {
		dataPVC, err = m.ensureSourcePVC(ctx)
	} else {
		dataPVC, err = m.ensureDestinationPVC(ctx)
	}
	if dataPVC == nil || err != nil {
		return mover.InProgress(), err
	}

	// Ensure service (if required) and publish the address in the status
	cont, err := m.ensureServiceAndPublishAddress(ctx)
	if !cont || err != nil {
		return mover.InProgress(), err
	}

	// Ensure Secrets/keys
	rsyncPSKSecretName, err := m.ensureSecrets(ctx)
	if rsyncPSKSecretName == nil || err != nil {
		return mover.InProgress(), err
	}

	// Prepare ServiceAccount, role, rolebinding
	sa, err := m.saHandler.Reconcile(ctx, m.logger)
	if sa == nil || err != nil {
		return mover.InProgress(), err
	}

	// clusterID is extracted from storageclass
	clusterID, err := m.clusterIDFromStorageClass(
		ctx, dataPVC.Spec.StorageClassName,
	)
	if err != nil {
		return mover.InProgress(), err
	}

	// Ensure ceph-csi ConfigMap in owner namespace
	csiConfigMapName, err := m.ensureCephCSIConfigMap(ctx, clusterID)
	if csiConfigMapName == nil || err != nil {
		return mover.InProgress(), err
	}

	// Ensure ceph-csi Secret in owner namespace
	csiSecretName, err := m.ensureCephCSISecret(ctx, clusterID)
	if csiSecretName == nil || err != nil {
		return mover.InProgress(), err
	}

	// Ensure mover Job
	job, err := m.ensureJob(
		ctx, dataPVC, sa, *rsyncPSKSecretName,
		*csiConfigMapName, *csiSecretName,
	)
	if job == nil || err != nil {
		return mover.InProgress(), err
	}

	// On the destination, preserve the image and return it
	if !m.isSource {
		image, err := m.vh.EnsureImage(ctx, m.logger, dataPVC)
		if image == nil || err != nil {
			return mover.InProgress(), err
		}
		return mover.CompleteWithImage(image), nil
	}

	// On the source, just signal completion
	return mover.Complete(), nil
}

// Cleanup transitions snapshot statuses, removes snapshot annotations,
// and deletes temporary resources marked for cleanup.
func (m *Mover) Cleanup(ctx context.Context) (mover.Result, error) {
	m.logger.V(1).Info("Starting cleanup", "m.mainPVCName", m.mainPVCName, "m.isSource", m.isSource)

	// Step 1 & 2: Transition snapshot statuses (mark old previous for cleanup, current -> previous)
	if err := m.transitionSnapshotStatuses(ctx); err != nil {
		return mover.InProgress(), err
	}

	// Step 3: Remove snapshot annotations (destination only)
	if !m.isSource {
		m.logger.V(1).Info("removing snapshot annotations from pvc")
		destPVCName := m.destPVCName
		if err := m.vh.RemoveSnapshotAnnotationFromPVC(ctx, m.logger, destPVCName); err != nil {
			return mover.InProgress(), err
		}
	}

	// Step 4: Delete marked objects
	if err := utils.CleanupObjects(ctx, m.client, m.logger, m.owner, cleanupTypes); err != nil {
		return mover.InProgress(), err
	}

	m.logger.V(1).Info("Cleanup complete")
	return mover.Complete(), nil
}
