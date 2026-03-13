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
	"flag"
	"fmt"
	"strconv"
	"strings"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/backube/volsync/controllers/volumehandler"
	"github.com/go-logr/logr"
	"github.com/spf13/viper"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	cephPluginMover "github.com/RamenDR/ceph-volsync-plugin/internal/mover"
)

const (
	cephfsProviderName = "cephfs.csi.ceph.com"
	nfsProviderName    = "nfs.csi.ceph.com"
	cephMoverName      = "ceph"
	// defaultCephContainerImage is the default container image for the
	// ceph data mover
	defaultCephContainerImage = "quay.io/ramendr/ceph-volsync-plugin-mover:latest"
	// Command line flag will be checked first
	// If command line flag not set, the MOVER_IMAGE env var will be used
	cephContainerImageFlag   = "mover-image"
	cephContainerImageEnvVar = "MOVER_IMAGE"

	// External parameter keys
	optStorageClassName        = "storageClassName"
	optVolumeSnapshotClassName = "volumeSnapshotClassName"
	optCopyMethod              = "copyMethod"
	optSecretKey               = "secretKey"
	optAddress                 = "address"
	optPort                    = "port"
	optKeySecret               = "keySecret"
	optServiceType             = "serviceType"
	optDestinationPVC          = "destinationPVC"
	optVolumeName              = "volumeName"
	optBaseSnapshotName        = "baseSnapshotName"
	optTargetSnapshotName      = "targetSnapshotName"
)

type Builder struct {
	viper *viper.Viper  // For unit tests to be able to override - global viper will be used by default in Register()
	flags *flag.FlagSet // For unit tests to be able to override - global flags will be used by default in Register()
}

var _ mover.Builder = &Builder{}

func Register() error {
	// Use global viper & command line flags
	b, err := newBuilder(viper.GetViper(), flag.CommandLine)
	if err != nil {
		return err
	}

	cephPluginMover.Register(b)
	return nil
}

func newBuilder(viper *viper.Viper, flags *flag.FlagSet) (*Builder, error) {
	b := &Builder{
		viper: viper,
		flags: flags,
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

func (rb *Builder) Name() string { return cephMoverName }

func (rb *Builder) VersionInfo() string {
	return fmt.Sprintf("Ceph container: %s", rb.getCephContainerImage())
}

// cephContainerImage is the container image name of the ceph data mover
func (rb *Builder) getCephContainerImage() string {
	return rb.viper.GetString(cephContainerImageFlag)
}

func (rb *Builder) FromSource(client client.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	source *volsyncv1alpha1.ReplicationSource, privileged bool) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if source.Spec.External == nil {
		return nil, nil
	}
	provider := source.Spec.External.Provider
	if !(strings.HasSuffix(provider, cephfsProviderName) ||
		strings.HasSuffix(provider, nfsProviderName)) {
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

	copyMethod := volsyncv1alpha1.CopyMethodNone
	rawCopyMethod, ok := source.Spec.External.Parameters[optCopyMethod]
	if ok {
		copyMethod = volsyncv1alpha1.CopyMethodType(rawCopyMethod)
	}

	var (
		storageClassName, volumeSnapshotClassName, secretKey, address *string
		port                                                          *int32
	)

	storageClassNameStr, ok := options[optStorageClassName]
	if ok {
		storageClassName = &storageClassNameStr
	}
	volumeSnapshotClassNameStr, ok := options[optVolumeSnapshotClassName]
	if ok {
		volumeSnapshotClassName = &volumeSnapshotClassNameStr
	}
	secretKeyStr, ok := options[optSecretKey]
	if ok {
		secretKey = &secretKeyStr
	}
	addressStr, ok := options[optAddress]
	if ok {
		address = &addressStr
	}
	portStr, ok := options[optPort]
	if ok {
		portInt, err := strconv.ParseInt(portStr, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid port value: %w", err)
		}
		portInt32 := int32(portInt)
		port = &portInt32
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(client),
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

	saHandler := utils.NewSAHandler(client, source, isSource, privileged,
		nil)

	return &Mover{
		client:            client,
		logger:            logger.WithValues("method", "Ceph"),
		eventRecorder:     eventRecorder,
		owner:             source,
		vh:                vh,
		saHandler:         saHandler,
		containerImage:    rb.getCephContainerImage(),
		key:               secretKey,
		address:           address,
		port:              port,
		isSource:          isSource,
		paused:            source.Spec.Paused,
		mainPVCName:       &source.Spec.SourcePVC,
		privileged:        privileged,
		sourceStatus:      source.Status.RsyncTLS,
		latestMoverStatus: source.Status.LatestMoverStatus,
		moverConfig:       volsyncv1alpha1.MoverConfig{},
		options:           source.Spec.External.Parameters,
	}, nil
}

//nolint:funlen
func (rb *Builder) FromDestination(client client.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	destination *volsyncv1alpha1.ReplicationDestination, privileged bool) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if destination.Spec.External == nil {
		return nil, nil
	}
	provider := destination.Spec.External.Provider

	if !(strings.HasSuffix(provider, cephfsProviderName) ||
		strings.HasSuffix(provider, nfsProviderName)) {
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

	copyMethod := volsyncv1alpha1.CopyMethodNone
	rawCopyMethod, ok := options[optCopyMethod]
	if ok {
		copyMethod = volsyncv1alpha1.CopyMethodType(rawCopyMethod)
	}

	var (
		storageClassName, volumeSnapshotClassName, keySecret *string
		serviceType                                          *corev1.ServiceType
		serviceAnnotations                                   map[string]string
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
	serviceTypeStr, ok := options[optServiceType]
	if ok {
		svcType := corev1.ServiceType(serviceTypeStr)
		serviceType = &svcType
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(client),
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

	saHandler := utils.NewSAHandler(client, destination, isSource, privileged,
		nil) // No specific SA for ceph mover

	var destPVC = options[optDestinationPVC]

	return &Mover{
		client:             client,
		logger:             logger.WithValues("method", "Ceph"),
		eventRecorder:      eventRecorder,
		owner:              destination,
		vh:                 vh,
		saHandler:          saHandler,
		containerImage:     rb.getCephContainerImage(),
		key:                keySecret,
		serviceType:        serviceType,
		serviceAnnotations: serviceAnnotations,
		isSource:           isSource,
		paused:             destination.Spec.Paused,
		mainPVCName:        &destPVC,
		cleanupTempPVC:     false, // Not applicable for ceph
		privileged:         privileged,
		destStatus:         destination.Status.RsyncTLS,
		latestMoverStatus:  destination.Status.LatestMoverStatus,
		moverConfig:        volsyncv1alpha1.MoverConfig{},
		options:            options,
	}, nil
}
