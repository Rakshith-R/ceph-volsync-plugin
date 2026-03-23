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
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	"github.com/backube/volsync/controllers/utils"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/config"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

// updateStatusPSK sets the PSK secret name in the source or destination status.
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
			Name:      m.namePrefix + m.owner.GetName(),
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
	cmName := m.namePrefix + "csi-config-" + m.direction + "-" + m.owner.GetName()
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
		return nil, fmt.Errorf("failed to read %s: %w", configPath, err)
	}
	data["config.json"] = string(configContent)

	// cluster-mapping.json is optional
	mappingPath := csiConfigMountPath +
		"/cluster-mapping.json"
	mappingContent, err := os.ReadFile(mappingPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read %s: %w", mappingPath, err)
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
	if m.moverType == constant.MoverRBD {
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
	newName := m.namePrefix +
		"csi-secret-" + m.direction + "-" +
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
			// Mount only userID and userKey directly
			// so the worker reads them as files without
			// JSON parsing or temp file creation.
			newSecret.Data = map[string][]byte{
				"userID":  origSecret.Data["userID"],
				"userKey": origSecret.Data["userKey"],
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
