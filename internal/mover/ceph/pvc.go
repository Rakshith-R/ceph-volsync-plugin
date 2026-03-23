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
	"errors"

	vserrors "github.com/backube/volsync/controllers/errors"
	"github.com/backube/volsync/controllers/utils"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ensureSourcePVC returns the source PVC. For Snapshot copy method, it creates
// a ReadOnlyMany PVC from a snapshot. For Direct, it returns the source PVC as-is.
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

// ensureDestinationPVC returns the destination PVC, either user-provided or newly allocated.
func (m *Mover) ensureDestinationPVC(ctx context.Context) (*corev1.PersistentVolumeClaim, error) {
	if m.destPVCIsProvided {
		return m.vh.UseProvidedPVC(ctx, m.destPVCName)
	}
	// Need to allocate the incoming data volume
	return m.vh.EnsureNewPVC(ctx, m.logger, m.destPVCName, m.cleanupTempPVC)
}
