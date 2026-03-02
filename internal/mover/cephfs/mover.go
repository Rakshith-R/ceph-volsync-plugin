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
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/backube/volsync/controllers/mover/rsynctls"
	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
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
	mountPath                = "/data"
	devicePath               = "/dev/block"
	dataVolumeName           = "data"
	tlsContainerPort         = 8000
	defaultServerStunnelPort = "8000"
	defaultServerPort        = "8080"
	defaultRsyncStunnelPort  = "8873"
	defaultRsyncPort         = "8874"

	volSyncCephFSPrefix = mover.VolSyncPrefix + "cephfs-"

	// MoverClusterRoleNameEnvVar is the environment variable name for the mover ClusterRole
	MoverClusterRoleNameEnvVar = "MOVER_CLUSTER_ROLE_NAME"
	// DefaultMoverClusterRoleName is the default name of the ClusterRole for mover jobs
	DefaultMoverClusterRoleName = "mover-role"

	// ClusterRoleBindingLabelKey is the label key used to identify ClusterRoleBindings created for mover jobs
	// The value is the owner's UID
	ClusterRoleBindingLabelKey = utils.VolsyncLabelPrefix + "/mover-clusterrolebinding-owner-uid"

	// Environment variable names for ConfigMap configuration
	configMapNameEnvVar      = "CEPH_CSI_CONFIGMAP_NAME"
	configMapNamespaceEnvVar = "CEPH_CSI_CONFIGMAP_NAMESPACE"
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

	// Ensure mover ClusterRoleBinding for secrets/configmaps access
	if err := m.ensureMoverClusterRoleBinding(ctx, sa); err != nil {
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

// ensureMoverClusterRoleBinding creates a ClusterRoleBinding that binds the mover's ServiceAccount
// to the mover-role ClusterRole, allowing access to secrets and configmaps.
func (m *Mover) ensureMoverClusterRoleBinding(ctx context.Context, sa *corev1.ServiceAccount) error {
	clusterRoleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: volSyncCephFSPrefix + m.owner.GetNamespace() + "-" + m.direction() + "-" + m.owner.GetName() + "-mover",
		},
	}
	logger := m.logger.WithValues("ClusterRoleBinding", clusterRoleBinding.Name)

	op, err := ctrlutil.CreateOrUpdate(ctx, m.client, clusterRoleBinding, func() error {
		// Add owner UID label
		if clusterRoleBinding.Labels == nil {
			clusterRoleBinding.Labels = make(map[string]string)
		}
		clusterRoleBinding.Labels[ClusterRoleBindingLabelKey] = string(m.owner.GetUID())

		// Get ClusterRole name from environment variable or use default
		clusterRoleName := os.Getenv(MoverClusterRoleNameEnvVar)
		if clusterRoleName == "" {
			clusterRoleName = DefaultMoverClusterRoleName
		}

		clusterRoleBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     clusterRoleName,
		}
		clusterRoleBinding.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      sa.Name,
				Namespace: sa.Namespace,
			},
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "ClusterRoleBinding reconcile failed")
		return err
	}

	logger.V(1).Info("Mover ClusterRoleBinding reconciled", "operation", op)
	return nil
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
		serverPort := defaultServerStunnelPort
		if m.port != nil {
			serverPort = strconv.Itoa(int(*m.port))
		}
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "DESTINATION_PORT",
			Value: serverPort,
		})

		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "SERVER_PORT",
			Value: defaultServerPort,
		})

		// Enable rsync tunnel by default for CephFS mover
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "ENABLE_RSYNC_TUNNEL",
			Value: "true",
		})
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "RSYNC_PORT",
			Value: defaultRsyncStunnelPort,
		})
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  "RSYNC_DAEMON_PORT",
			Value: defaultRsyncPort,
		})

		containerEnv = append(containerEnv, corev1.EnvVar{Name: "VOLUME_HANDLE", Value: dataPVC.Spec.VolumeName})
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		})

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
		}

		volumeEnv, err := m.getVolumeEnvVars(ctx, dataPVC)
		if err != nil {
			return fmt.Errorf("failed to get volume env vars: %w", err)
		}
		containerEnv = append(containerEnv, volumeEnv...)

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
			Name:  "cephfs-mover",
			Env:   containerEnv,
			Image: m.containerImage,
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr.To(true),
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
				Privileged:             ptr.To(true),
				ReadOnlyRootFilesystem: ptr.To(true),
				RunAsUser:              ptr.To[int64](0), // Run as root for stunnel
			},
		}}
		// Pre-allocate with capacity for better performance (max 5 items)
		volumeMounts := make([]corev1.VolumeMount, 0, 5)
		if !blockVolume {
			volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: dataVolumeName, MountPath: mountPath})
		}
		volumeMounts = append(volumeMounts, corev1.VolumeMount{Name: "keys", MountPath: "/keys"},
			corev1.VolumeMount{Name: "tempdir", MountPath: "/tmp"},
			corev1.VolumeMount{Name: "ceph-config", MountPath: "/etc/ceph"},
			corev1.VolumeMount{Name: "ceph-csi-config", MountPath: "/etc/ceph-csi-config"})
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
			{Name: "ceph-config", VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{}},
			},
			{Name: "ceph-csi-config", VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{}},
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
