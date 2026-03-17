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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/config"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker"
	wcommon "github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
	"github.com/backube/volsync/controllers/mover/rsynctls"
	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
}

var _ mover.Mover = &Mover{}

// All object types that are temporary/per-iteration should be listed here. The
// individual objects to be cleaned up must also be marked.
var cleanupTypes = []client.Object{
	&corev1.PersistentVolumeClaim{},
	&snapv1.VolumeSnapshot{},
	&batchv1.Job{},
}

func (m *Mover) Name() string { return cephMoverName }

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

func (m *Mover) ensureServiceAndPublishAddress(ctx context.Context) (bool, error) {
	if m.address != nil || m.isSource {
		// Connection will be outbound. Don't need a Service
		return true, nil
	}

	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.moverPrefix() + m.direction() + "-" + m.owner.GetName(),
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
		MoverType:   m.moverType,
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
			Name:      m.moverPrefix() + m.owner.GetName(),
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

// ensureCephCSIConfigMap reads the ceph-csi config files
// from the operator's mounted ConfigMap and creates a
// per-RS/RD ConfigMap in the owner's namespace.
// TODO: filter the config for only the relevant(+mapped)
// clusterID instead of copying everything.
func (m *Mover) ensureCephCSIConfigMap(
	ctx context.Context,
	_ string,
) (*string, error) {
	cmName := m.moverPrefix() +
		"csi-config-" + m.direction() + "-" +
		m.owner.GetName()
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: m.owner.GetNamespace(),
		},
	}
	logger := m.logger.WithValues("ConfigMap", cmName)

	data := make(map[string]string)

	// config.json is required
	configPath := csiConfigMountPath + "/config.json"
	configContent, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read %s: %w",
			configPath, err,
		)
	}
	data["config.json"] = string(configContent)

	// cluster-mapping.json is optional
	mappingPath := csiConfigMountPath +
		"/cluster-mapping.json"
	mappingContent, err := os.ReadFile(mappingPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf(
				"failed to read %s: %w",
				mappingPath, err,
			)
		}
		logger.Info(
			"cluster-mapping.json not found, skipping",
			"path", mappingPath,
		)
	} else {
		data["cluster-mapping.json"] =
			string(mappingContent)
	}

	op, err := ctrlutil.CreateOrUpdate(
		ctx, m.client, cm, func() error {
			if err := ctrl.SetControllerReference(
				m.owner, cm, m.client.Scheme(),
			); err != nil {
				logger.Error(
					err,
					utils.ErrUnableToSetControllerRef,
				)
				return err
			}
			utils.SetOwnedByVolSync(cm)
			cm.Data = data
			return nil
		},
	)
	if err != nil {
		logger.Error(err, "ConfigMap reconcile failed")
		return nil, err
	}

	logger.V(1).Info(
		"CSI ConfigMap reconciled", "operation", op,
	)
	return &cmName, nil
}

// ensureCephCSISecret extracts clusterID from the PVC,
// looks up the ceph admin secret ref from csi config,
// fetches it, and creates a copy in the owner's namespace.
func (m *Mover) ensureCephCSISecret(
	ctx context.Context,
	clusterID string,
) (*string, error) {
	if m.mainPVCName == nil {
		return nil, fmt.Errorf("mainPVCName is not set")
	}

	// Get the source PVC to find its PV
	srcPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      *m.mainPVCName,
			Namespace: m.owner.GetNamespace(),
		},
	}
	if err := m.client.Get(
		ctx, client.ObjectKeyFromObject(srcPVC), srcPVC,
	); err != nil {
		return nil, fmt.Errorf(
			"failed to get PVC: %w", err,
		)
	}

	// Look up the secret ref from csi config
	getSecretRef := config.GetCephFSControllerPublishSecretRef
	if m.moverType == MoverTypeRBD {
		getSecretRef = config.GetRBDControllerPublishSecretRef
	}
	secretName, secretNS, err :=
		getSecretRef(config.CsiConfigFile, clusterID)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get secret ref: %w", err,
		)
	}

	// Fetch the original secret
	origSecret := &corev1.Secret{}
	if err := m.client.Get(ctx, client.ObjectKey{
		Name:      secretName,
		Namespace: secretNS,
	}, origSecret); err != nil {
		return nil, fmt.Errorf(
			"failed to fetch ceph secret %s/%s: %w",
			secretNS, secretName, err,
		)
	}

	// Create/update a copy in the owner's namespace
	newName := m.moverPrefix() +
		"csi-secret-" + m.direction() + "-" +
		m.owner.GetName()
	newSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      newName,
			Namespace: m.owner.GetNamespace(),
		},
	}
	logger := m.logger.WithValues("Secret", newName)

	op, err := ctrlutil.CreateOrUpdate(
		ctx, m.client, newSecret, func() error {
			if err := ctrl.SetControllerReference(
				m.owner, newSecret, m.client.Scheme(),
			); err != nil {
				logger.Error(
					err,
					utils.ErrUnableToSetControllerRef,
				)
				return err
			}
			utils.SetOwnedByVolSync(newSecret)
			// Store secret entries as a single JSON
			// file so the mover reads one file instead
			// of iterating over directory entries.
			secretMap := make(
				map[string]string,
				len(origSecret.Data),
			)
			for k, v := range origSecret.Data {
				secretMap[k] = string(v)
			}
			jsonBytes, err := json.Marshal(secretMap)
			if err != nil {
				return fmt.Errorf(
					"failed to marshal secret: %w",
					err,
				)
			}
			newSecret.Data = map[string][]byte{
				worker.CsiSecretJSONKey: jsonBytes,
			}
			return nil
		},
	)
	if err != nil {
		logger.Error(err, "CSI Secret reconcile failed")
		return nil, err
	}

	logger.V(1).Info(
		"CSI Secret reconciled", "operation", op,
	)
	return &newName, nil
}

func (m *Mover) direction() string {
	dir := "src"
	if !m.isSource {
		dir = "dst"
	}
	return dir
}

func (m *Mover) moverPrefix() string {
	if m.moverType == MoverTypeRBD {
		return mover.VolSyncPrefix + "rbd-"
	}
	return volSyncCephFSPrefix
}

func (m *Mover) containerName() string {
	if m.moverType == MoverTypeRBD {
		return "rbd-mover"
	}
	return "cephfs-mover"
}

func (m *Mover) serviceSelector() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      m.direction() + "-" + m.owner.GetName(),
		"app.kubernetes.io/component": m.containerName(),
		"app.kubernetes.io/part-of":   "ceph-volsync-plugin",
	}
}

// clusterIDFromStorageClass fetches the named StorageClass and
// returns the value of its "clusterID" parameter.
func (m *Mover) clusterIDFromStorageClass(
	ctx context.Context,
	scName *string,
) (string, error) {
	if scName == nil {
		return "", fmt.Errorf("storageClassName is not set")
	}
	sc := &storagev1.StorageClass{}
	if err := m.client.Get(
		ctx, client.ObjectKey{Name: *scName}, sc,
	); err != nil {
		return "", fmt.Errorf(
			"failed to get StorageClass %s: %w",
			*scName, err,
		)
	}
	clusterID, ok := sc.Parameters["clusterID"]
	if !ok {
		return "", fmt.Errorf(
			"clusterID not found in StorageClass %s",
			*scName,
		)
	}
	return clusterID, nil
}

func (m *Mover) Cleanup(ctx context.Context) (mover.Result, error) {
	m.logger.V(1).Info("Starting cleanup", "m.mainPVCName", m.mainPVCName, "m.isSource", m.isSource)

	// Step 1 & 2: Transition snapshot statuses (mark old previous for cleanup, current -> previous)
	if err := m.transitionSnapshotStatuses(ctx); err != nil {
		return mover.InProgress(), err
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
	if err := utils.CleanupObjects(ctx, m.client, m.logger, m.owner, cleanupTypes); err != nil {
		return mover.InProgress(), err
	}

	m.logger.V(1).Info("Cleanup complete")
	return mover.Complete(), nil
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
func (m *Mover) ensureJob(
	ctx context.Context,
	dataPVC *corev1.PersistentVolumeClaim,
	sa *corev1.ServiceAccount,
	rsyncSecretName, csiConfigMapName,
	csiSecretName string,
) (*batchv1.Job, error) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.moverPrefix() + m.direction() + "-" + m.owner.GetName(),
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

		job.Spec.Template.Name = job.Name
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

		// Add WORKER_TYPE environment variable
		workerType := "destination"
		if m.isSource {
			workerType = "source"
		}
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  worker.EnvWorkerType,
			Value: workerType,
		})

		// Add DESTINATION_PORT for stunnel configuration
		serverPort := wcommon.DefaultServerStunnelPort
		if m.port != nil {
			serverPort = strconv.Itoa(int(*m.port))
		}
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  worker.EnvDestinationPort,
			Value: serverPort,
		})

		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  worker.EnvServerPort,
			Value: wcommon.DefaultServerPort,
		})

		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  worker.EnvLogLevel,
			Value: "info",
		})

		// Set MOVER_TYPE for the worker
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "MOVER_TYPE",
			Value: string(m.moverType),
		})

		// CephFS-specific: rsync port configuration
		if m.moverType == MoverTypeCephFS {
			containerEnv = append(containerEnv, corev1.EnvVar{
				Name:  worker.EnvRsyncPort,
				Value: wcommon.DefaultRsyncStunnelPort,
			})
			containerEnv = append(containerEnv, corev1.EnvVar{
				Name:  worker.EnvRsyncDaemonPort,
				Value: wcommon.DefaultRsyncDaemonPort,
			})
		}

		containerEnv = append(containerEnv, corev1.EnvVar{
			Name: worker.EnvPodNamespace,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		})

		if m.isSource {
			// Set dest address/port if necessary
			if m.address != nil {
				containerEnv = append(containerEnv, corev1.EnvVar{Name: worker.EnvDestinationAddress, Value: *m.address})
			}
			if m.port != nil {
				connectPort := strconv.Itoa(int(*m.port))
				containerEnv = append(containerEnv, corev1.EnvVar{Name: worker.EnvDestinationPort, Value: connectPort})
			}

			// Set read-only for volume in repl source job spec if the PVC only supports read-only
			readOnlyVolume = utils.PvcIsReadOnly(dataPVC)
		}

		volumeEnv, err := m.getVolumeEnvVars(ctx, dataPVC)
		if err != nil {
			return fmt.Errorf("failed to get volume env vars: %w", err)
		}
		containerEnv = append(containerEnv, volumeEnv...)

		podSpec := &job.Spec.Template.Spec
		podSpec.Containers = []corev1.Container{{
			Name:  m.containerName(),
			Env:   containerEnv,
			Image: m.containerImage,
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr.To(true),
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
				Privileged:             ptr.To(true),
				ReadOnlyRootFilesystem: ptr.To(true),
				RunAsUser:              ptr.To[int64](0), // Run as root for stunnel and rsync
			},
		}}
		volumeMounts := make([]corev1.VolumeMount, 0, 6)
		if !blockVolume {
			volumeMounts = append(volumeMounts,
				corev1.VolumeMount{
					Name:      dataVolumeName,
					MountPath: wcommon.DataMountPath,
				})
		}
		volumeMounts = append(volumeMounts,
			corev1.VolumeMount{
				Name:      "keys",
				MountPath: "/keys",
			},
			corev1.VolumeMount{
				Name:      "tempdir",
				MountPath: "/tmp",
			},
			corev1.VolumeMount{
				Name:      "ceph-config",
				MountPath: "/etc/ceph",
			},
			corev1.VolumeMount{
				Name:      "ceph-csi-config",
				MountPath: csiConfigMountPath,
				ReadOnly:  true,
			},
			corev1.VolumeMount{
				Name:      csiSecretVolumeName,
				MountPath: worker.CsiSecretMountPath,
				ReadOnly:  true,
			},
		)
		job.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
		if blockVolume {
			job.Spec.Template.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
				{Name: dataVolumeName, DevicePath: wcommon.DevicePath},
			}
		}
		podSpec.RestartPolicy = corev1.RestartPolicyNever
		podSpec.ServiceAccountName = sa.Name
		podSpec.Volumes = []corev1.Volume{
			{Name: dataVolumeName,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: dataPVC.Name,
						ReadOnly:  readOnlyVolume,
					},
				}},
			{Name: "keys",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  rsyncSecretName,
						DefaultMode: ptr.To[int32](0600),
					},
				}},
			{Name: "tempdir",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{
						Medium: corev1.StorageMediumMemory,
					},
				}},
			{Name: "ceph-config",
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				}},
			{Name: "ceph-csi-config",
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: csiConfigMapName,
						},
					},
				}},
			{Name: csiSecretVolumeName,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName:  csiSecretName,
						DefaultMode: ptr.To[int32](0600),
					},
				}},
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
				Name:  worker.EnvPrivilegedMover,
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
				Name:  worker.EnvPrivilegedMover,
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
