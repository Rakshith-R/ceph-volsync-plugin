//go:build !disable_cephfs

/*
Copyright 2024 The VolSync authors.

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published
by the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package cephfs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"time"

	"github.com/RamenDR/ceph-volsync-plugin/internal/mover/rsynctls"
	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	vserrors "github.com/backube/volsync/controllers/errors"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/backube/volsync/controllers/volumehandler"
)

const (
	mountPath         = "/data"
	devicePath        = "/dev/block"
	dataVolumeName    = "data"
	tlsContainerPort  = 8000
	defaultServerPort = "8080"
	defaultRsyncPort  = "8873"

	volSyncCephFSPrefix = mover.VolSyncPrefix + "cephfs-"

	// cleanupLabelKey matches the private constant in utils package
	// We need it here to query snapshots with cleanup label
	cleanupLabelKey          = utils.VolsyncLabelPrefix + "/cleanup"
	preserveLastSnapLabelKey = utils.VolsyncLabelPrefix + "/preserve-last-snapshot"

	// Snapshot status labels for lifecycle management
	snapshotStatusCurrent  = "current"
	snapshotStatusPrevious = "previous"

	// Environment variable names for ConfigMap configuration
	configMapNameEnvVar      = "CEPH_CSI_CONFIGMAP_NAME"
	configMapNamespaceEnvVar = "CEPH_CSI_CONFIGMAP_NAMESPACE"
)

// snapshotStatusLabelKey returns the label key for snapshot status, including owner name for easier identification
func (m *Mover) snapshotStatusLabelKey() string {
	return utils.VolsyncLabelPrefix + "/snapshot-status-" + m.owner.GetName()
}

// Mover is the reconciliation logic for the CephFS-based data mover.
type Mover struct {
	client             client.Client
	logger             logr.Logger
	eventRecorder      events.EventRecorder
	owner              client.Object
	vh                 *volumehandler.VolumeHandler
	saHandler          utils.SAHandler
	containerImage     string
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
}

var _ mover.Mover = &Mover{}

// All object types that are temporary/per-iteration should be listed here. The
// individual objects to be cleaned up must also be marked.
var cleanupTypes = []client.Object{
	&corev1.PersistentVolumeClaim{},
	&snapv1.VolumeSnapshot{},
	&batchv1.Job{},
}

func (m *Mover) Name() string { return cephfsMoverName }

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

	// Ensure mover Job
	job, err := m.ensureJob(ctx, dataPVC, sa, *rsyncPSKSecretName)
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

func (m *Mover) ensureServiceAndPublishAddress(ctx context.Context) (bool, error) {
	if m.address != nil || m.isSource {
		// Connection will be outbound. Don't need a Service
		return true, nil
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volSyncCephFSPrefix + m.direction() + "-" + m.owner.GetName(),
			Namespace: m.owner.GetNamespace(),
		},
	}
	svcDesc := rsyncSvcDescription{
		Context:     ctx,
		Client:      m.client,
		Service:     service,
		Owner:       m.owner,
		Type:        m.serviceType,
		Selector:    m.serviceSelector(),
		Port:        m.port,
		Annotations: m.serviceAnnotations,
	}
	err := svcDesc.Reconcile(m.logger)
	if err != nil {
		return false, err
	}

	return m.publishSvcAddress(service)
}

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

func (m *Mover) updateStatusPSK(pskSecretName *string) {
	if m.isSource {
		m.sourceStatus.KeySecret = pskSecretName
	} else {
		m.destStatus.KeySecret = pskSecretName
	}
}

// Will ensure the secret exists or create secrets if necessary
// - Returns the name of the secret that should be used in the replication job
func (m *Mover) ensureSecrets(ctx context.Context) (*string, error) {
	// If user provided key, use that
	if m.key != nil {
		keySecret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      *m.key,
				Namespace: m.owner.GetNamespace(),
			},
		}
		fields := []string{"psk.txt"}
		if err := utils.GetAndValidateSecret(ctx, m.client, m.logger, keySecret, fields...); err != nil {
			m.logger.Error(err, "Key Secret does not contain the proper fields")
			return nil, err
		}
		return m.key, nil
	}

	keySecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volSyncCephFSPrefix + m.owner.GetName(),
			Namespace: m.owner.GetNamespace(),
		},
	}

	err := m.client.Get(ctx, client.ObjectKeyFromObject(keySecret), keySecret)
	if client.IgnoreNotFound(err) != nil {
		m.logger.Error(err, "error retrieving key")
		return nil, err
	}

	if kerrors.IsNotFound(err) {
		keyData := make([]byte, 64)
		if _, err := rand.Read(keyData); err != nil {
			m.logger.Error(err, "error generating key")
			return nil, err
		}
		keySecret.StringData = map[string]string{
			"psk.txt": "volsync:" + hex.EncodeToString(keyData),
		}
		if err := ctrl.SetControllerReference(m.owner, keySecret, m.client.Scheme()); err != nil {
			m.logger.Error(err, utils.ErrUnableToSetControllerRef)
			return nil, err
		}
		utils.SetOwnedByVolSync(keySecret)

		if err := m.client.Create(ctx, keySecret); err != nil {
			m.logger.Error(err, "error creating key Secret")
			return nil, err
		}
	}

	m.updateStatusPSK(&keySecret.Name)
	return &keySecret.Name, nil
}

func (m *Mover) direction() string {
	dir := "src"
	if !m.isSource {
		dir = "dst"
	}
	return dir
}

func (m *Mover) serviceSelector() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      m.direction() + "-" + m.owner.GetName(),
		"app.kubernetes.io/component": "cephfs-mover",
		"app.kubernetes.io/part-of":   "ceph-volsync-plugin",
	}
}

func (m *Mover) Cleanup(ctx context.Context) (mover.Result, error) {
	m.logger.V(1).Info("Starting cleanup", "m.mainPVCName", m.mainPVCName, "m.isSource", m.isSource)

	// Step 1: Mark all snapshots with status=previous for deletion
	previousSnaps, err := m.listSnapshotsWithStatus(ctx, snapshotStatusPrevious)
	if err != nil {
		m.logger.Error(err, "failed to list previous snapshots")
		return mover.InProgress(), err
	}
	for i := range previousSnaps {
		snap := &previousSnaps[i]
		updated := false
		updated = utils.MarkForCleanup(m.owner, snap) // Ignore return value, we want to mark all previous snaps
		updated = updated || utils.RemoveLabel(snap, m.snapshotStatusLabelKey())
		if updated {
			if err := m.client.Update(ctx, snap); err != nil {
				m.logger.Error(err, "failed to mark previous snapshot for cleanup", "snapshot", snap.Name)
				return mover.InProgress(), err
			}
			m.logger.V(1).Info("Marked previous snapshot for cleanup", "snapshot", snap.Name)
		}

	}

	// Step 2: Transition status=current to status=previous
	currentSnap, err := m.findSnapshotWithStatus(ctx, snapshotStatusCurrent)
	if err != nil {
		m.logger.Error(err, "failed to find current snapshot")
		return mover.InProgress(), err
	}
	if currentSnap != nil {
		if err = m.setSnapshotStatus(ctx, currentSnap, snapshotStatusPrevious); err != nil {
			m.logger.Error(err, "failed to transition current snapshot to previous", "snapshot", currentSnap.Name)
			return mover.InProgress(), err
		}
		m.logger.V(1).Info("Transitioned current snapshot to previous", "snapshot", currentSnap.Name)
	}

	// Step 3: Remove snapshot annotations (destination only)
	if !m.isSource {
		m.logger.V(1).Info("removing snapshot annotations from pvc")
		_, destPVCName := m.getDestinationPVCName()
		if err := m.vh.RemoveSnapshotAnnotationFromPVC(ctx, m.logger, destPVCName); err != nil {
			return mover.InProgress(), err
		}
	}

	// Step 4: Delete marked objects
	if err = utils.CleanupObjects(ctx, m.client, m.logger, m.owner, cleanupTypes); err != nil {
		return mover.InProgress(), err
	}

	m.logger.V(1).Info("Cleanup complete")
	return mover.Complete(), nil
}

// isSnapshotReady checks if a snapshot is ready to use
func isSnapshotReady(snap *snapv1.VolumeSnapshot) bool {
	if snap == nil {
		return false
	}
	return snap.Status != nil && snap.Status.ReadyToUse != nil && *snap.Status.ReadyToUse
}

// findSnapshotWithStatus finds a snapshot with specific status label
func (m *Mover) findSnapshotWithStatus(ctx context.Context, status string) (*snapv1.VolumeSnapshot, error) {
	snapshots, err := m.listSnapshotsWithStatus(ctx, status)
	if err != nil || len(snapshots) == 0 {
		return nil, err
	}
	return &snapshots[0], nil
}

// listSnapshotsWithStatus lists all snapshots with specific status label
func (m *Mover) listSnapshotsWithStatus(ctx context.Context, status string) ([]snapv1.VolumeSnapshot, error) {
	selector, err := labels.Parse(m.snapshotStatusLabelKey() + "=" + status)
	if err != nil {
		return nil, err
	}

	listOptions := []client.ListOption{
		client.MatchingLabelsSelector{Selector: selector},
		client.InNamespace(m.owner.GetNamespace()),
	}

	snapList := &snapv1.VolumeSnapshotList{}
	if err := m.client.List(ctx, snapList, listOptions...); err != nil {
		return nil, err
	}

	return snapList.Items, nil
}

// setSnapshotStatus updates the snapshot status label
func (m *Mover) setSnapshotStatus(ctx context.Context, snap *snapv1.VolumeSnapshot, status string) error {
	if snap == nil {
		return nil
	}

	updated := utils.AddLabel(snap, m.snapshotStatusLabelKey(), status)
	if !updated {
		return nil // Label already set to desired value
	}

	return m.client.Update(ctx, snap)
}

// ensureSnapshotWithStatusLabel creates or gets a snapshot with status=current label
func (m *Mover) ensureSnapshotWithStatusLabel(ctx context.Context, logger logr.Logger,
	src *corev1.PersistentVolumeClaim, name string) (*snapv1.VolumeSnapshot, error) {
	snapshot := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: m.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, m.client, snapshot, func() error {
		if err := ctrl.SetControllerReference(m.owner, snapshot, m.client.Scheme()); err != nil {
			return err
		}
		utils.SetOwnedByVolSync(snapshot)
		utils.MarkForCleanup(m.owner, snapshot)
		utils.AddLabel(snapshot, m.snapshotStatusLabelKey(), snapshotStatusCurrent)

		// Set snapshot spec if creating
		if snapshot.CreationTimestamp.IsZero() {
			snapshot.Spec.Source.PersistentVolumeClaimName = &src.Name
			snapshot.Spec.VolumeSnapshotClassName = src.Spec.StorageClassName
		}

		return nil
	})

	if err != nil {
		logger.Error(err, "failed to create/update snapshot with status label", "snapshot", name)
		return nil, err
	}

	logger.V(1).Info("Snapshot reconciled with status label", "operation", op, "snapshot", name)

	// Check if snapshot is ready
	if !isSnapshotReady(snapshot) {
		return nil, nil // Not ready yet, will retry
	}

	return snapshot, nil
}

// createPVCFromSnapshot creates or gets a PVC from a snapshot
func (m *Mover) createPVCFromSnapshot(ctx context.Context, logger logr.Logger,
	snap *snapv1.VolumeSnapshot, src *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snap.Name,
			Namespace: m.owner.GetNamespace(),
		},
	}

	op, err := ctrlutil.CreateOrUpdate(ctx, m.client, pvc, func() error {
		if err := ctrl.SetControllerReference(m.owner, pvc, m.client.Scheme()); err != nil {
			return err
		}
		utils.SetOwnedByVolSync(pvc)
		utils.MarkForCleanup(m.owner, pvc)

		// Set PVC spec if creating
		if pvc.CreationTimestamp.IsZero() {
			pvc.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany}
			pvc.Spec.Resources = src.Spec.Resources
			pvc.Spec.StorageClassName = src.Spec.StorageClassName
			pvc.Spec.VolumeMode = src.Spec.VolumeMode

			// Set DataSource to the snapshot
			pvc.Spec.DataSource = &corev1.TypedLocalObjectReference{
				APIGroup: &snapv1.SchemeGroupVersion.Group,
				Kind:     "VolumeSnapshot",
				Name:     snap.Name,
			}
		}

		return nil
	})

	if err != nil {
		logger.Error(err, "failed to create/update PVC from snapshot", "pvc", pvc.Name)
		return nil, err
	}

	logger.V(1).Info("PVC reconciled from snapshot", "operation", op, "pvc", pvc.Name)

	// Check if PVC is bound
	if pvc.Status.Phase != corev1.ClaimBound {
		return nil, nil // Not bound yet, will retry
	}

	return pvc, nil
}

// ensurePVCFromSrcWithStatusLabels creates a PVC from source using status-labeled snapshots
func (m *Mover) ensurePVCFromSrcWithStatusLabels(ctx context.Context, logger logr.Logger,
	src *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	// Look for snapshot with status=current
	currentSnap, err := m.findSnapshotWithStatus(ctx, snapshotStatusCurrent)
	if err != nil {
		return nil, err
	}

	var snap *snapv1.VolumeSnapshot

	if currentSnap != nil {
		if isSnapshotReady(currentSnap) {
			// Reuse existing ready snapshot
			logger.V(1).Info("Reusing existing current snapshot", "snapshot", currentSnap.Name)
			snap = currentSnap
		} else {
			// Wait for current snapshot to become ready
			return nil, nil
		}
	} else {
		// Create new snapshot with status=current
		suffix := strconv.FormatInt(time.Now().Unix(), 10)
		dataName := mover.VolSyncPrefix + m.owner.GetName() + "-" + m.direction() + "-" + suffix

		logger.V(1).Info("Creating new snapshot with status=current", "name", dataName)
		snap, err = m.ensureSnapshotWithStatusLabel(ctx, logger, src, dataName)
		if snap == nil || err != nil {
			return nil, err
		}
	}

	return m.createPVCFromSnapshot(ctx, logger, snap, src)
}

func (m *Mover) ensureSourcePVC(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
	srcPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *m.mainPVCName,
			Namespace: m.owner.GetNamespace(),
		},
	}
	if err := m.client.Get(ctx, client.ObjectKeyFromObject(srcPVC), srcPVC); err != nil {
		m.logger.Error(err, "unable to get source PVC", "PVC", client.ObjectKeyFromObject(srcPVC))
		return nil, err
	}
	if m.vh.IsCopyMethodDirect() {
		return srcPVC, nil
	}

	pvc, err := m.ensurePVCFromSrcWithStatusLabels(ctx, m.logger, srcPVC)
	if err != nil {
		// If the error was a copy TriggerTimeoutError, update the latestMoverStatus to indicate error
		var copyTriggerTimeoutError *vserrors.CopyTriggerTimeoutError
		if errors.As(err, &copyTriggerTimeoutError) {
			utils.UpdateMoverStatusFailed(m.latestMoverStatus, copyTriggerTimeoutError.Error())
			// Don't return error - we want to keep reconciling at the normal in-progress rate
			// but just indicate in the latestMoverStatus that there is an error (we've been waiting
			// for the user to update the copy Trigger for too long)
			return pvc, nil
		}
		return nil, err
	}
	return pvc, nil
}

func (m *Mover) ensureDestinationPVC(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
	isProvidedPVC, dataPVCName := m.getDestinationPVCName()
	if isProvidedPVC {
		return m.vh.UseProvidedPVC(ctx, dataPVCName)
	}
	// Need to allocate the incoming data volume
	return m.vh.EnsureNewPVC(ctx, m.logger, dataPVCName, m.cleanupTempPVC)
}

func (m *Mover) getDestinationPVCName() (bool, string) {
	if m.mainPVCName == nil {
		newPvcName := mover.VolSyncPrefix + m.owner.GetName() + "-" + m.direction()
		return false, newPvcName
	}
	return true, *m.mainPVCName
}

//nolint:funlen
func (m *Mover) ensureJob(ctx context.Context, dataPVC *corev1.PersistentVolumeClaim,
	sa *corev1.ServiceAccount, rsyncSecretName string) (*batchv1.Job, error) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      volSyncCephFSPrefix + m.direction() + "-" + m.owner.GetName(),
			Namespace: m.owner.GetNamespace(),
		},
	}
	logger := m.logger.WithValues("job", client.ObjectKeyFromObject(job))

	op, err := utils.CreateOrUpdateDeleteOnImmutableErr(ctx, m.client, job, logger, func() error {
		if err := ctrl.SetControllerReference(m.owner, job, m.client.Scheme()); err != nil {
			logger.Error(err, utils.ErrUnableToSetControllerRef)
			return err
		}
		utils.SetOwnedByVolSync(job)
		utils.MarkForCleanup(m.owner, job)

		job.Spec.Template.ObjectMeta.Name = job.Name
		utils.AddAllLabels(&job.Spec.Template, m.serviceSelector())
		utils.SetOwnedByVolSync(&job.Spec.Template) // ensure the Job's Pod gets the ownership label
		backoffLimit := int32(2)
		job.Spec.BackoffLimit = &backoffLimit

		parallelism := int32(1)
		if m.paused {
			parallelism = int32(0)
		}
		job.Spec.Parallelism = &parallelism

		readOnlyVolume := false
		blockVolume := utils.PvcIsBlockMode(dataPVC)

		// Pre-allocate with capacity for better performance (base 7 + conditionals)
		containerEnv := make([]corev1.EnvVar, 0, 12)

		// Add WORKER_TYPE environment variable for stunnel.sh
		workerType := "destination"
		if m.isSource {
			workerType = "source"
		}
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "WORKER_TYPE",
			Value: workerType,
		})

		// Add SERVER_PORT for stunnel.sh configuration
		serverPort := defaultServerPort
		if m.port != nil {
			serverPort = strconv.Itoa(int(*m.port))
		}
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "SERVER_PORT",
			Value: serverPort,
		})

		// Enable rsync tunnel by default for CephFS mover
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "ENABLE_RSYNC_TUNNEL",
			Value: "true",
		})
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "RSYNC_PORT",
			Value: defaultRsyncPort,
		})
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "RSYNC_DAEMON_PORT",
			Value: defaultRsyncPort,
		})

		containerEnv = append(containerEnv, corev1.EnvVar{Name: "VOLUME_HANDLE", Value: dataPVC.Spec.VolumeName})

		if m.isSource {
			// Set dest address/port if necessary
			if m.address != nil {
				containerEnv = append(containerEnv, corev1.EnvVar{Name: "DESTINATION_ADDRESS", Value: *m.address})
			}
			if m.port != nil {
				connectPort := strconv.Itoa(int(*m.port))
				containerEnv = append(containerEnv, corev1.EnvVar{Name: "DESTINATION_PORT", Value: connectPort})
			}

			// Set read-only for volume in repl source job spec if the PVC only supports read-only
			readOnlyVolume = utils.PvcIsReadOnly(dataPVC)

			// targetSnapshotHandle
			if targetSnapshotHandle, ok := m.options["targetSnapshotHandle"]; ok {
				containerEnv = append(containerEnv, corev1.EnvVar{Name: "TARGET_SNAPSHOT_HANDLE", Value: targetSnapshotHandle})
			}
			// baseSnapshotHandle
			if baseSnapshotHandle, ok := m.options["baseSnapshotHandle"]; ok {
				containerEnv = append(containerEnv, corev1.EnvVar{Name: "BASE_SNAPSHOT_HANDLE", Value: baseSnapshotHandle})
			}
		}

		// Inject ConfigMap configuration from environment variables
		if configMapName := os.Getenv(configMapNameEnvVar); configMapName != "" {
			containerEnv = append(containerEnv, corev1.EnvVar{
				Name:  configMapNameEnvVar,
				Value: configMapName,
			})
		}
		if configMapNamespace := os.Getenv(configMapNamespaceEnvVar); configMapNamespace != "" {
			containerEnv = append(containerEnv, corev1.EnvVar{
				Name:  configMapNamespaceEnvVar,
				Value: configMapNamespace,
			})
		}

		podSpec := &job.Spec.Template.Spec
		podSpec.Containers = []corev1.Container{{
			Name:  "rsync-tls",
			Env:   containerEnv,
			Image: m.containerImage,
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr.To(false),
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
				Privileged:             ptr.To(false),
				ReadOnlyRootFilesystem: ptr.To(true),
				RunAsUser:              ptr.To[int64](0), // Run as root for stunnel
			},
		}}
		// Pre-allocate with capacity for better performance (max 3 items)
		volumeMounts := make([]corev1.VolumeMount, 0, 3)
		if !blockVolume {
			volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: dataVolumeName, MountPath: mountPath})
		}
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "keys", MountPath: "/keys"},
			corev1.VolumeMount{Name: "tempdir", MountPath: "/tmp"})
		job.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
		if blockVolume {
			job.Spec.Template.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
				{Name: dataVolumeName, DevicePath: devicePath},
			}
		}
		podSpec.RestartPolicy = corev1.RestartPolicyNever
		podSpec.ServiceAccountName = sa.Name
		podSpec.Volumes = []corev1.Volume{
			{Name: dataVolumeName, VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: dataPVC.Name,
					ReadOnly:  readOnlyVolume,
				}},
			},
			{Name: "keys", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  rsyncSecretName,
					DefaultMode: ptr.To[int32](0600),
				}},
			},
			{Name: "tempdir", VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					Medium: corev1.StorageMediumMemory,
				}},
			},
		}
		if m.vh.IsCopyMethodDirect() {
			affinity, err := utils.AffinityFromVolume(ctx, m.client, logger, dataPVC)
			if err != nil {
				logger.Error(err, "unable to determine proper affinity", "PVC", client.ObjectKeyFromObject(dataPVC))
				return err
			}
			podSpec.NodeSelector = affinity.NodeSelector
			podSpec.Tolerations = affinity.Tolerations
		}

		// Update the job securityContext, podLabels and resourceRequirements from moverConfig (if specified)
		utils.UpdatePodTemplateSpecFromMoverConfig(&job.Spec.Template, m.moverConfig, corev1.ResourceRequirements{})

		if m.privileged {
			podSpec.Containers[0].Env = append(podSpec.Containers[0].Env, corev1.EnvVar{
				Name:  "PRIVILEGED_MOVER",
				Value: "1",
			})
			podSpec.Containers[0].SecurityContext.Capabilities.Add = []corev1.Capability{
				"DAC_OVERRIDE", // Read/write all files
				"CHOWN",        // chown files
				"FOWNER",       // Set permission bits & times
				"SETGID",       // Set process GID/supplemental groups
			}
			podSpec.Containers[0].SecurityContext.RunAsUser = ptr.To[int64](0)
		} else {
			podSpec.Containers[0].Env = append(podSpec.Containers[0].Env, corev1.EnvVar{
				Name:  "PRIVILEGED_MOVER",
				Value: "0",
			})
		}

		// Run mover in debug mode if required
		podSpec.Containers[0].Env = utils.AppendDebugMoverEnvVar(m.owner, podSpec.Containers[0].Env)

		logger.V(1).Info("Job has PVC", "PVC", dataPVC, "DS", dataPVC.Spec.DataSource)
		return nil
	})
	// If Job had failed, delete it so it can be recreated
	if job.Status.Failed >= *job.Spec.BackoffLimit {
		// Update status with mover logs from failed job
		utils.UpdateMoverStatusForFailedJob(ctx, m.logger, m.latestMoverStatus, job.GetName(), job.GetNamespace(),
			rsynctls.LogLineFilterFailure)

		logger.Info("deleting job -- backoff limit reached")
		m.eventRecorder.Eventf(m.owner, job, corev1.EventTypeWarning,
			volsyncv1alpha1.EvRTransferFailed, volsyncv1alpha1.EvADeleteMover, "mover Job backoff limit reached")
		err = m.client.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
		return nil, err
	}
	if err != nil {
		logger.Error(err, "reconcile failed")
		return nil, err
	}

	logger.V(1).Info("Job reconciled", "operation", op)
	if op == ctrlutil.OperationResultCreated {
		dir := "receive"
		if m.isSource {
			dir = "transmit"
		}
		m.eventRecorder.Eventf(m.owner, job, corev1.EventTypeNormal,
			volsyncv1alpha1.EvRTransferStarted, volsyncv1alpha1.EvACreateMover, "starting %s to %s data",
			utils.KindAndName(m.client.Scheme(), job), dir)
	}

	// Stop here if the job hasn't completed yet
	if job.Status.Succeeded == 0 {
		return nil, nil
	}

	logger.Info("job completed")

	// update status with mover logs from successful job
	utils.UpdateMoverStatusForSuccessfulJob(ctx, m.logger, m.latestMoverStatus, job.GetName(), job.GetNamespace(),
		rsynctls.LogLineFilterSuccess)

	// We only continue reconciling if the rsync job has completed
	return job, nil
}
