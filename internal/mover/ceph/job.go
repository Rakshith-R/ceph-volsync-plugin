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
	"fmt"
	"strconv"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover/rsynctls"
	"github.com/backube/volsync/controllers/utils"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

// ensureJob creates or updates the mover Job with all required env vars,
// volume mounts, and security context. Returns the Job on successful completion.
//
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
			Name:      m.namePrefix + m.direction + "-" + m.owner.GetName(),
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
		utils.AddAllLabels(&job.Spec.Template, m.serviceSelector)
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
		workerType := constant.WorkerDestination
		if m.isSource {
			workerType = constant.WorkerSource
		}
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  constant.EnvWorkerType,
			Value: string(workerType),
		})

		// Add DESTINATION_PORT for stunnel configuration
		serverPort := strconv.Itoa(int(constant.TLSPort))
		if m.port != nil {
			serverPort = strconv.Itoa(int(*m.port))
		}
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  constant.EnvDestinationPort,
			Value: serverPort,
		})

		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  constant.EnvLogLevel,
			Value: "info",
		})

		// Set MOVER_TYPE for the worker
		containerEnv = append(containerEnv, corev1.EnvVar{
			Name:  constant.EnvMoverType,
			Value: string(m.moverType),
		})

		containerEnv = append(containerEnv, corev1.EnvVar{
			Name: constant.EnvPodNamespace,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		})

		if m.isSource {
			// Set dest address/port if necessary
			if m.address != nil {
				containerEnv = append(containerEnv, corev1.EnvVar{Name: constant.EnvDestinationAddress, Value: *m.address})
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
			Name:  m.containerName,
			Env:   containerEnv,
			Image: m.containerImage,
			SecurityContext: &corev1.SecurityContext{
				AllowPrivilegeEscalation: ptr.To(true),
				Capabilities: &corev1.Capabilities{
					Drop: []corev1.Capability{"ALL"},
				},
				// container does not need to be privileged
				// but needs privilegeesclation to set necessary
				// capabilities file related operations.
				Privileged:             ptr.To(false),
				ReadOnlyRootFilesystem: ptr.To(true),
				RunAsUser:              ptr.To[int64](0), // Run as root for stunnel and rsync
			},
		}}
		volumeMounts := make([]corev1.VolumeMount, 0, 6)
		if !blockVolume {
			volumeMounts = append(volumeMounts,
				corev1.VolumeMount{
					Name:      dataVolumeName,
					MountPath: constant.DataMountPath,
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
				MountPath: constant.CsiSecretMountPath,
				ReadOnly:  true,
			},
		)
		job.Spec.Template.Spec.Containers[0].VolumeMounts = volumeMounts
		if blockVolume {
			job.Spec.Template.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
				{Name: dataVolumeName, DevicePath: constant.DevicePath},
			}
		}
		podSpec.RestartPolicy = corev1.RestartPolicyNever
		if m.isSource {
			// hostnetworking is required for the source mover to connect to
			// ceph daemons running on hostnetwork.
			podSpec.HostNetwork = true
			podSpec.DNSPolicy = corev1.DNSClusterFirstWithHostNet
		}
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
				Name:  constant.EnvPrivilegedMover,
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
				Name:  constant.EnvPrivilegedMover,
				Value: "0",
			})
		}

		// Run mover in debug mode if required
		podSpec.Containers[0].Env = utils.AppendDebugMoverEnvVar(m.owner, podSpec.Containers[0].Env)

		var dsName string
		if ds := dataPVC.Spec.DataSource; ds != nil {
			dsName = ds.Kind + "/" + ds.Name
		}
		logger.V(1).Info("Job has PVC",
			"PVC", client.ObjectKeyFromObject(dataPVC),
			"DS", dsName)
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
