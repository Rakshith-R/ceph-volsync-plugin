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
)

const (
	dataVolumeName = "data"

	volSyncCephFSPrefix = mover.VolSyncPrefix + "cephfs-"

	// Paths for ceph-csi config mounted in the operator
	csiConfigMountPath = "/etc/ceph-csi-config"

	// Volume name for ceph-csi secret
	csiSecretVolumeName = "ceph-csi-secret" //nolint:gosec // G101: volume name, not credentials
)

// MoverType represents the type of data mover.
type MoverType string

const (
	MoverTypeCephFS MoverType = "cephfs"
	MoverTypeRBD    MoverType = "rbd"
)

// Mover is the reconciliation logic for the CephFS-based data mover.
type Mover struct {
	client             client.Client
	logger             logr.Logger
	eventRecorder      events.EventRecorder
	owner              client.Object
	vh                 *volumehandler.VolumeHandler
	saHandler          utils.SAHandler
	containerImage     string
	moverType          MoverType
	key                *string
	serviceType        *corev1.ServiceType
	serviceAnnotations map[string]string
	address            *string
	port               *int32
	isSource           bool
	paused             bool
	mainPVCName        *string
	privileged         bool
	latestMoverStatus  *volsyncv1alpha1.MoverStatus
	moverConfig        volsyncv1alpha1.MoverConfig
	// Source-only fields
	sourceStatus *volsyncv1alpha1.ReplicationSourceRsyncTLSStatus
	// Destination-only fields
	destStatus     *volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus
	cleanupTempPVC bool
	options        map[string]string

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
	if m.moverType == MoverTypeRBD {
		m.namePrefix = mover.VolSyncPrefix + "rbd-"
		m.containerName = "rbd-mover"
	} else {
		m.namePrefix = volSyncCephFSPrefix
		m.containerName = "cephfs-mover"
	}
	m.serviceSelector = map[string]string{
		"app.kubernetes.io/name":      m.direction + "-" + m.owner.GetName(),
		"app.kubernetes.io/component": m.containerName,
		"app.kubernetes.io/part-of":   "ceph-volsync-plugin",
	}
	m.snapStatusLabelKey = utils.VolsyncLabelPrefix +
		"/snapshot-status-" + m.owner.GetName()
	if m.mainPVCName == nil {
		m.destPVCIsProvided = false
		m.destPVCName = mover.VolSyncPrefix +
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
