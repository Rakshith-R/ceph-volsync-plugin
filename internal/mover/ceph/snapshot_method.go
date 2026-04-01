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
	"time"

	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

const (
	// Snapshot status labels for lifecycle management
	snapshotStatusCurrent  = "current"
	snapshotStatusPrevious = "previous"
)

// isSnapshotReady returns true if the snapshot exists and its ReadyToUse status is true.
func isSnapshotReady(snap *snapv1.VolumeSnapshot) bool {
	return snap != nil && snap.Status != nil &&
		snap.Status.ReadyToUse != nil && *snap.Status.ReadyToUse
}

// findCurrentSnapshot returns the first snapshot with
// "current" status label, or nil if none found.
func (m *Mover) findCurrentSnapshot(
	ctx context.Context,
) (*snapv1.VolumeSnapshot, error) {
	snapshots, err := m.listSnapshotsWithStatus(
		ctx, snapshotStatusCurrent,
	)
	if err != nil || len(snapshots) == 0 {
		return nil, err
	}
	return &snapshots[0], nil
}

// listSnapshotsWithStatus lists all snapshots in the owner's namespace with the given status label.
func (m *Mover) listSnapshotsWithStatus(ctx context.Context, status string) ([]snapv1.VolumeSnapshot, error) {
	selector, err := labels.Parse(m.snapStatusLabelKey + "=" + status)
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

// setSnapshotStatus adds or updates the snapshot status label. No-op if snap is nil
// or the label already has the desired value.
func (m *Mover) setSnapshotStatus(
	ctx context.Context, snap *snapv1.VolumeSnapshot, status string,
) error {
	if snap == nil {
		return nil
	}

	updated := utils.AddLabel(snap, m.snapStatusLabelKey, status)
	if !updated {
		return nil // Label already set to desired value
	}

	return m.client.Update(ctx, snap)
}

// ensureSnapshotWithStatusLabel creates or updates a VolumeSnapshot with
// status=current label and waits for it to become ready.
func (m *Mover) ensureSnapshotWithStatusLabel(
	ctx context.Context, logger logr.Logger,
	src *corev1.PersistentVolumeClaim, name string,
) (*snapv1.VolumeSnapshot, error) {
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
		utils.AddLabel(snapshot, m.snapStatusLabelKey, snapshotStatusCurrent)

		vsClassName := m.options[optVolumeSnapshotClassName]
		// Set snapshot spec if creating
		if snapshot.CreationTimestamp.IsZero() {
			snapshot.Spec.Source.PersistentVolumeClaimName = &src.Name
			snapshot.Spec.VolumeSnapshotClassName = &vsClassName
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

// createPVCFromSnapshot creates a ReadOnlyMany PVC from a snapshot and
// waits for it to become Bound.
func (m *Mover) createPVCFromSnapshot(
	ctx context.Context, logger logr.Logger,
	snap *snapv1.VolumeSnapshot, src *corev1.PersistentVolumeClaim,
) (*corev1.PersistentVolumeClaim, error) {
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

// ensurePVCFromSrcWithStatusLabels reuses an existing current snapshot or creates
// a new one with a timestamp suffix, then creates a PVC from it.
func (m *Mover) ensurePVCFromSrcWithStatusLabels(
	ctx context.Context, logger logr.Logger,
	src *corev1.PersistentVolumeClaim,
) (*corev1.PersistentVolumeClaim, error) {
	// Look for snapshot with status=current
	currentSnap, err := m.findCurrentSnapshot(ctx)
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
		dataName := mover.VolSyncPrefix + m.owner.GetName() + "-" + m.direction + "-" + suffix

		logger.V(1).Info("Creating new snapshot with status=current", "name", dataName)
		snap, err = m.ensureSnapshotWithStatusLabel(ctx, logger, src, dataName)
		if snap == nil || err != nil {
			return nil, err
		}
	}

	return m.createPVCFromSnapshot(ctx, logger, snap, src)
}

// getVolumeEnvVars extracts volume and snapshot handles for the worker container.
// For source movers with previous snapshots, it computes base/target snapshot handles
// for incremental sync.
func (m *Mover) getVolumeEnvVars(
	ctx context.Context,
	dataPVC *corev1.PersistentVolumeClaim,
) ([]corev1.EnvVar, error) {
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
	envVars := []corev1.EnvVar{}

	if !m.isSource {
		// For destination, just add volumeHandle.
		volumeHandle, err := m.getVolumeHandle(ctx, dataPVC.Spec.VolumeName)
		if err != nil {
			return envVars, err
		}

		envVars = append(envVars,
			corev1.EnvVar{
				Name:  constant.EnvVolumeHandle,
				Value: volumeHandle,
			},
		)
		return envVars, nil
	}

	if m.vh.IsCopyMethodDirect() {
		// # baseSnapshotName: <base-snapshot-name> # required for Diff sync
		// # targetSnapshotName: <target-snapshot-name> # required for Diff sync
		// # parentVolumeName: <parent-volume-name> # required for Diff sync
		volumeName := m.options[optVolumeName]
		baseSnapshotName := m.options[optBaseSnapshotName]
		targetSnapshotName := m.options[optTargetSnapshotName]
		if volumeName == "" || baseSnapshotName == "" || targetSnapshotName == "" {
			volumeHandle, err := m.getVolumeHandle(ctx, dataPVC.Spec.VolumeName)
			if err != nil {
				return envVars, err
			}

			envVars = append(envVars,
				corev1.EnvVar{
					Name:  constant.EnvVolumeHandle,
					Value: volumeHandle,
				},
			)
			return envVars, nil
		}
		pvc := &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName,
				Namespace: m.owner.GetNamespace(),
			},
		}
		if err := m.client.Get(ctx, client.ObjectKeyFromObject(pvc), pvc); err != nil {
			m.logger.Error(err, "unable to get source PVC", "PVC", client.ObjectKeyFromObject(srcPVC))
			return nil, err
		}
		// fetch Handles
		volumeHandle, err := m.getVolumeHandle(ctx, pvc.Spec.VolumeName)
		if err != nil {
			return envVars, err
		}
		baseSnapshotHandle, err := m.getSnapshotHandle(ctx, baseSnapshotName)
		if err != nil {
			return envVars, err
		}
		targetSnapshotHandle, err := m.getSnapshotHandle(ctx, targetSnapshotName)
		if err != nil {
			return envVars, err
		}
		envVars = append(envVars,
			corev1.EnvVar{
				Name:  constant.EnvVolumeHandle,
				Value: volumeHandle,
			},
			corev1.EnvVar{
				Name:  constant.EnvBaseSnapshotHandle,
				Value: baseSnapshotHandle,
			},
			corev1.EnvVar{
				Name:  constant.EnvTargetSnapshotHandle,
				Value: targetSnapshotHandle,
			},
		)

		return envVars, nil
	}

	previousSnapList, err := m.listSnapshotsWithStatus(ctx, snapshotStatusPrevious)
	if err != nil {
		return envVars, err
	}
	if len(previousSnapList) == 0 {
		// For destination, just add volumeHandle.
		volumeHandle, err := m.getVolumeHandle(ctx, srcPVC.Spec.VolumeName)
		if err != nil {
			return envVars, err
		}

		envVars = append(envVars,
			corev1.EnvVar{
				Name:  constant.EnvVolumeHandle,
				Value: volumeHandle,
			},
		)

		return envVars, nil
	}

	previousSnap := &previousSnapList[0]
	baseSnapHandle, err := m.getSnapshotHandle(ctx, previousSnap.Name)
	if err != nil {
		return envVars, err
	}
	targetSnapshot, err := m.findCurrentSnapshot(ctx)
	if err != nil {
		return envVars, err
	}
	if targetSnapshot == nil {
		return envVars, fmt.Errorf("no current snapshot found for target snapshot handle")
	}
	targetSnapHandle, err := m.getSnapshotHandle(ctx, targetSnapshot.Name)
	if err != nil {
		return envVars, err
	}

	volumeHandle, err := m.getVolumeHandle(ctx, srcPVC.Spec.VolumeName)
	if err != nil {
		return envVars, err
	}
	envVars = append(envVars,
		corev1.EnvVar{
			Name:  constant.EnvVolumeHandle,
			Value: volumeHandle,
		},
		corev1.EnvVar{
			Name:  constant.EnvBaseSnapshotHandle,
			Value: baseSnapHandle,
		},
		corev1.EnvVar{
			Name:  constant.EnvTargetSnapshotHandle,
			Value: targetSnapHandle,
		},
	)

	return envVars, nil
}

// getVolumeHandle fetches the PV by name and returns its CSI VolumeHandle.
func (m *Mover) getVolumeHandle(
	ctx context.Context, volumeName string,
) (string, error) {
	pv := &corev1.PersistentVolume{}
	err := m.client.Get(ctx, client.ObjectKey{
		Name: volumeName,
	},
		pv)
	if err != nil {
		return "", err
	}

	if pv.Status.Phase != corev1.VolumeBound {
		return "", fmt.Errorf("PV %s is not bound", pv.Name)
	}

	if pv.Spec.CSI == nil {
		return "", fmt.Errorf("PV %s is not a CSI volume", pv.Name)
	}

	return pv.Spec.CSI.VolumeHandle, nil
}

// getSnapshotHandle fetches a VolumeSnapshot, retrieves its BoundVolumeSnapshotContent,
// and returns the underlying SnapshotHandle.
func (m *Mover) getSnapshotHandle(
	ctx context.Context, snapshotName string,
) (string, error) {
	snapshot := &snapv1.VolumeSnapshot{}
	err := m.client.Get(ctx, client.ObjectKey{
		Name:      snapshotName,
		Namespace: m.owner.GetNamespace(),
	},
		snapshot)
	if err != nil {
		return "", err
	}

	if !isSnapshotReady(snapshot) {
		return "", fmt.Errorf("snapshot %s is not ready", snapshot.Name)
	}

	if snapshot.Status == nil || snapshot.Status.BoundVolumeSnapshotContentName == nil {
		return "", fmt.Errorf("snapshot %s has no bound content", snapshot.Name)
	}

	// Fetch VolumeSnapshotContent to get the handle
	content := &snapv1.VolumeSnapshotContent{}
	err = m.client.Get(ctx, client.ObjectKey{
		Name: *snapshot.Status.BoundVolumeSnapshotContentName,
	}, content)
	if err != nil {
		return "", err
	}

	if content.Status == nil || content.Status.SnapshotHandle == nil {
		return "", fmt.Errorf("snapshot content %s has no handle", content.Name)
	}

	return *content.Status.SnapshotHandle, nil
}

// transitionSnapshotStatuses rotates snapshot lifecycle labels: marks old previous
// snapshots for cleanup (keeping the most recent), then transitions current to previous.
func (m *Mover) transitionSnapshotStatuses(
	ctx context.Context,
) error {
	// Step 1: Mark all snapshots with status=previous for deletion, preserving the most recent
	previousSnaps, err := m.listSnapshotsWithStatus(ctx, snapshotStatusPrevious)
	if err != nil {
		m.logger.Error(err, "failed to list previous snapshots")
		return err
	}

	// Preserve the most recent previous snapshot, mark others for cleanup
	if len(previousSnaps) > 1 {
		latestSnapIndex := 0
		latestSnapTime := previousSnaps[0].CreationTimestamp
		for i := 1; i < len(previousSnaps); i++ {
			if previousSnaps[i].CreationTimestamp.After(latestSnapTime.Time) {
				latestSnapIndex = i
				latestSnapTime = previousSnaps[i].CreationTimestamp
			}
		}
		for i := range previousSnaps {
			if i == latestSnapIndex {
				continue
			}
			snap := &previousSnaps[i]
			updated := utils.MarkForCleanup(m.owner, snap)
			updated = updated || utils.RemoveLabel(snap, m.snapStatusLabelKey)
			if updated {
				if err := m.client.Update(ctx, snap); err != nil {
					m.logger.Error(err, "failed to mark previous snapshot for cleanup", "snapshot", snap.Name)
					return err
				}
				m.logger.V(1).Info("Marked previous snapshot for cleanup", "snapshot", snap.Name)
			}
		}
	}

	// Step 2: Transition status=current to status=previous
	currentSnap, err := m.findCurrentSnapshot(ctx)
	if err != nil {
		m.logger.Error(err, "failed to find current snapshot")
		return err
	}
	if currentSnap != nil {
		if err = m.setSnapshotStatus(ctx, currentSnap, snapshotStatusPrevious); err != nil {
			m.logger.Error(err, "failed to transition current snapshot to previous", "snapshot", currentSnap.Name)
			return err
		}
		m.logger.V(1).Info("Transitioned current snapshot to previous", "snapshot", currentSnap.Name)
	}

	return nil
}
