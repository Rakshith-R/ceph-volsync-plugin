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
	"flag"
	"fmt"
	"os"
	"strings"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/backube/volsync/controllers/volumehandler"
	"github.com/go-logr/logr"
	"github.com/spf13/viper"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cephpluginmover "github.com/RamenDR/ceph-volsync-plugin/internal/mover"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

const (
	cephfsProviderName = "cephfs.csi.ceph.com"
	nfsProviderName    = "nfs.csi.ceph.com"
	rbdProviderName    = "rbd.csi.ceph.com"
	nvmeofProviderName = "nvmeof.csi.ceph.com"
	cephMoverName      = "ceph"
	// defaultCephContainerImage is the default container image for the
	// ceph data mover
	defaultCephContainerImage = "quay.io/ramendr/ceph-volsync-plugin-mover:latest"
	// Command line flag will be checked first
	// If command line flag not set, the MOVER_IMAGE env var will be used
	cephContainerImageFlag   = "mover-image"
	cephContainerImageEnvVar = "MOVER_IMAGE"

	// CSI config env vars for the operator to locate the ceph-csi ConfigMap
	csiConfigNameEnvVar      = "CEPH_CSI_CONFIG_NAME"
	csiConfigNamespaceEnvVar = "CEPH_CSI_CONFIG_NAMESPACE"
	defaultCSIConfigName     = "ceph-csi-config"
	defaultCSIConfigNS       = "rook-ceph"

	// External parameter keys
	// common parameters
	optStorageClassName        = "storageClassName"
	optVolumeSnapshotClassName = "volumeSnapshotClassName"
	optCopyMethod              = "copyMethod"
	// source-specific parameters
	optAddress   = "address"
	optKeySecret = "keySecret"

	// destination-specific parameters
	optDestinationPVC     = "destinationPVC"
	optVolumeName         = "volumeName"
	optBaseSnapshotName   = "baseSnapshotName"
	optTargetSnapshotName = "targetSnapshotName"
)

// Builder implements mover.Builder for the Ceph data mover.
// It detects the CSI provider from the CR and constructs Mover instances.
type Builder struct {
	viper              *viper.Viper  // For unit tests to be able to override - global viper will be used by default in Register()
	flags              *flag.FlagSet // For unit tests to be able to override - global flags will be used by default in Register()
	csiConfigName      string
	csiConfigNamespace string
}

var _ mover.Builder = &Builder{}

// Register creates a Builder with global viper/flags and adds it to the mover catalog.
func Register() error {
	// Use global viper & command line flags
	b, err := newBuilder(viper.GetViper(), flag.CommandLine)
	if err != nil {
		return err
	}

	cephpluginmover.Register(b)
	return nil
}

// newBuilder initializes a Builder with the given viper and flag set,
// registering the container image flag and env var binding.
func newBuilder(v *viper.Viper, flags *flag.FlagSet) (*Builder, error) {
	csiName := os.Getenv(csiConfigNameEnvVar)
	if csiName == "" {
		csiName = defaultCSIConfigName
	}
	csiNS := os.Getenv(csiConfigNamespaceEnvVar)
	if csiNS == "" {
		csiNS = defaultCSIConfigNS
	}
	b := &Builder{
		viper:              v,
		flags:              flags,
		csiConfigName:      csiName,
		csiConfigNamespace: csiNS,
	}

	// Set default ceph container image - will be used if both command line flag and env var are not set
	b.viper.SetDefault(cephContainerImageFlag, defaultCephContainerImage)

	// Setup command line flag for the ceph container image
	b.flags.String(cephContainerImageFlag, defaultCephContainerImage,
		"The container image for the ceph data mover")
	// Viper will check for command line flag first, then fallback to the env var
	err := b.viper.BindEnv(cephContainerImageFlag, cephContainerImageEnvVar)

	return b, err
}

// Name returns the builder's registered name ("ceph").
func (rb *Builder) Name() string { return cephMoverName }

// VersionInfo returns a human-readable string with the configured container image.
func (rb *Builder) VersionInfo() string {
	return fmt.Sprintf("Ceph container: %s", rb.getCephContainerImage())
}

// cephContainerImage is the container image name of the ceph data mover
func (rb *Builder) getCephContainerImage() string {
	return rb.viper.GetString(cephContainerImageFlag)
}

// FromSource constructs a Mover for a ReplicationSource CR.
// Returns (nil, nil) if the CR does not belong to a supported Ceph CSI provider.
func (rb *Builder) FromSource(cl client.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	source *volsyncv1alpha1.ReplicationSource, privileged bool) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if source.Spec.External == nil {
		return nil, nil
	}
	provider := source.Spec.External.Provider
	var mt constant.MoverType
	if strings.HasSuffix(provider, cephfsProviderName) ||
		strings.HasSuffix(provider, nfsProviderName) {
		mt = constant.MoverCephFS
	} else if strings.HasSuffix(provider, rbdProviderName) ||
		strings.HasSuffix(provider, nvmeofProviderName) {
		mt = constant.MoverRBD
	} else {
		return nil, nil
	}

	// Make sure there's a place to write status info
	if source.Status.RsyncTLS == nil {
		source.Status.RsyncTLS = &volsyncv1alpha1.ReplicationSourceRsyncTLSStatus{}
	}

	if source.Status.LatestMoverStatus == nil {
		source.Status.LatestMoverStatus = &volsyncv1alpha1.MoverStatus{}
	}

	options := source.Spec.External.Parameters
	if len(options) == 0 {
		return nil, fmt.Errorf("missing external parameters in ceph replication source")
	}

	// TODO: validate copy method and other parameters here before creating the Mover.
	copyMethod := volsyncv1alpha1.CopyMethodDirect
	rawCopyMethod, ok := source.Spec.External.Parameters[optCopyMethod]
	if ok {
		copyMethod = volsyncv1alpha1.CopyMethodType(rawCopyMethod)
	}

	var (
		storageClassName, volumeSnapshotClassName, secretKey, address *string
	)
	storageClassNameStr, ok := options[optStorageClassName]
	if ok {
		storageClassName = &storageClassNameStr
	}
	volumeSnapshotClassNameStr, ok := options[optVolumeSnapshotClassName]
	if ok {
		volumeSnapshotClassName = &volumeSnapshotClassNameStr
	}
	secretKeyStr, ok := options[optKeySecret]
	if ok {
		secretKey = &secretKeyStr
	}
	addressStr, ok := options[optAddress]
	if ok {
		address = &addressStr
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(cl),
		volumehandler.WithRecorder(eventRecorder),
		volumehandler.WithOwner(source),
		volumehandler.FromSource(&volsyncv1alpha1.ReplicationSourceVolumeOptions{
			CopyMethod:              copyMethod,
			StorageClassName:        storageClassName,
			VolumeSnapshotClassName: volumeSnapshotClassName,
		}),
	)
	if err != nil {
		return nil, err
	}

	isSource := true

	saHandler := utils.NewSAHandler(cl, source, isSource, privileged,
		nil)

	m := &Mover{
		client:             cl,
		logger:             logger.WithValues("method", "Ceph"),
		eventRecorder:      eventRecorder,
		owner:              source,
		vh:                 vh,
		saHandler:          saHandler,
		containerImage:     rb.getCephContainerImage(),
		moverType:          mt,
		key:                secretKey,
		address:            address,
		isSource:           isSource,
		paused:             source.Spec.Paused,
		mainPVCName:        &source.Spec.SourcePVC,
		privileged:         privileged,
		sourceStatus:       source.Status.RsyncTLS,
		latestMoverStatus:  source.Status.LatestMoverStatus,
		moverConfig:        volsyncv1alpha1.MoverConfig{},
		options:            source.Spec.External.Parameters,
		csiConfigName:      rb.csiConfigName,
		csiConfigNamespace: rb.csiConfigNamespace,
	}
	m.initCached()

	return m, nil
}

// FromDestination constructs a Mover for a ReplicationDestination CR.
// Returns (nil, nil) if the CR does not belong to a supported Ceph CSI provider.
//
//nolint:funlen
func (rb *Builder) FromDestination(cl client.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	destination *volsyncv1alpha1.ReplicationDestination, privileged bool) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if destination.Spec.External == nil {
		return nil, nil
	}
	provider := destination.Spec.External.Provider
	var mt constant.MoverType
	if strings.HasSuffix(provider, cephfsProviderName) ||
		strings.HasSuffix(provider, nfsProviderName) {
		mt = constant.MoverCephFS
	} else if strings.HasSuffix(provider, rbdProviderName) ||
		strings.HasSuffix(provider, nvmeofProviderName) {
		mt = constant.MoverRBD
	} else {
		return nil, nil
	}

	// Make sure there's a place to write status info
	if destination.Status.RsyncTLS == nil {
		destination.Status.RsyncTLS = &volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus{}
	}

	if destination.Status.LatestMoverStatus == nil {
		destination.Status.LatestMoverStatus = &volsyncv1alpha1.MoverStatus{}
	}

	options := destination.Spec.External.Parameters
	if len(options) == 0 {
		return nil, fmt.Errorf("missing external parameters in ceph replication destination")
	}

	// TODO: validate copy method and other parameters here before creating the Mover.
	copyMethod := volsyncv1alpha1.CopyMethodDirect
	rawCopyMethod, ok := options[optCopyMethod]
	if ok {
		copyMethod = volsyncv1alpha1.CopyMethodType(rawCopyMethod)
	}

	var (
		storageClassName, volumeSnapshotClassName, keySecret *string
	)
	storageClassNameStr, ok := options[optStorageClassName]
	if ok {
		storageClassName = &storageClassNameStr
	}
	volumeSnapshotClassNameStr, ok := options[optVolumeSnapshotClassName]
	if ok {
		volumeSnapshotClassName = &volumeSnapshotClassNameStr
	}
	keySecretStr, ok := options[optKeySecret]
	if ok {
		keySecret = &keySecretStr
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(cl),
		volumehandler.WithRecorder(eventRecorder),
		volumehandler.WithOwner(destination),
		volumehandler.FromDestination(&volsyncv1alpha1.ReplicationDestinationVolumeOptions{
			CopyMethod:              copyMethod,
			StorageClassName:        storageClassName,
			VolumeSnapshotClassName: volumeSnapshotClassName,
		}),
	)
	if err != nil {
		return nil, err
	}

	isSource := false
	saHandler := utils.NewSAHandler(cl, destination, isSource, privileged,
		nil)

	var destPVCPtr *string
	if destPVC, ok := options[optDestinationPVC]; ok {
		destPVCPtr = &destPVC
	}

	m := &Mover{
		client:             cl,
		logger:             logger.WithValues("method", "Ceph"),
		eventRecorder:      eventRecorder,
		owner:              destination,
		vh:                 vh,
		saHandler:          saHandler,
		containerImage:     rb.getCephContainerImage(),
		moverType:          mt,
		key:                keySecret,
		isSource:           isSource,
		paused:             destination.Spec.Paused,
		mainPVCName:        destPVCPtr,
		cleanupTempPVC:     false,
		privileged:         privileged,
		destStatus:         destination.Status.RsyncTLS,
		latestMoverStatus:  destination.Status.LatestMoverStatus,
		moverConfig:        volsyncv1alpha1.MoverConfig{},
		options:            options,
		csiConfigName:      rb.csiConfigName,
		csiConfigNamespace: rb.csiConfigNamespace,
	}
	m.initCached()

	return m, nil
}
