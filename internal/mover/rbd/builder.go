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

package rbd

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
	rbdProviderName    = "rbd.csi.ceph.com"
	nvmeofProviderName = "nvmeof.csi.ceph.com"
	rbdMoverName       = "rbd"
	// defaultRBDContainerImage is the default container image for the
	// rbd data mover
	defaultRBDContainerImage = "ghcr.io/rakshith-r/ceph-volsync-mover:s0.1"
	// Command line flag will be checked first
	// If command line flag not set, the RBD_MOVER_IMAGE env var will be used
	rbdContainerImageFlag   = "rbd-mover-image"
	rbdContainerImageEnvVar = "RBD_MOVER_IMAGE"
)

type Builder struct {
	viper *viper.Viper  // For unit tests to be able to override
	flags *flag.FlagSet // For unit tests to be able to override
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

	// Set default rbd container image
	b.viper.SetDefault(rbdContainerImageFlag, defaultRBDContainerImage)

	// Setup command line flag for the rbd container image
	b.flags.String(rbdContainerImageFlag, defaultRBDContainerImage,
		"The container image for the rbd data mover")
	// Viper will check for command line flag first, then fallback to the env var
	err := b.viper.BindEnv(rbdContainerImageFlag, rbdContainerImageEnvVar)

	return b, err
}

func (rb *Builder) Name() string { return rbdMoverName }

func (rb *Builder) VersionInfo() string {
	return fmt.Sprintf("RBD container: %s", rb.getRBDContainerImage())
}

// getRBDContainerImage is the container image name of the rbd data mover
func (rb *Builder) getRBDContainerImage() string {
	return rb.viper.GetString(rbdContainerImageFlag)
}

func (rb *Builder) FromSource(client client.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	source *volsyncv1alpha1.ReplicationSource, privileged bool,
) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if source.Spec.External == nil {
		return nil, nil
	}
	provider := source.Spec.External.Provider
	if !(strings.HasSuffix(provider, rbdProviderName) ||
		strings.HasSuffix(provider, nvmeofProviderName)) {
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
		return nil, fmt.Errorf("missing external parameters in rbd replication source")
	}

	copyMethod := volsyncv1alpha1.CopyMethodNone
	rawCopyMethod, ok := source.Spec.External.Parameters["copyMethod"]
	if ok {
		copyMethod = volsyncv1alpha1.CopyMethodType(rawCopyMethod)
	}

	var (
		storageClassName, volumeSnapshotClassName, secretKey, address *string
		port                                                          *int32
	)

	storageClassNameStr, ok := options["storageClassName"]
	if ok {
		storageClassName = &storageClassNameStr
	}
	volumeSnapshotClassNameStr, ok := options["volumeSnapshotClassName"]
	if ok {
		volumeSnapshotClassName = &volumeSnapshotClassNameStr
	}
	secretKeyStr, ok := options["secretKey"]
	if ok {
		secretKey = &secretKeyStr
	}
	addressStr, ok := options["address"]
	if ok {
		address = &addressStr
	}
	portStr, ok := options["port"]
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
		nil) // No specific SA for rbd mover

	return &Mover{
		client:            client,
		logger:            logger.WithValues("method", "RBD"),
		eventRecorder:     eventRecorder,
		owner:             source,
		vh:                vh,
		saHandler:         saHandler,
		containerImage:    rb.getRBDContainerImage(),
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
	destination *volsyncv1alpha1.ReplicationDestination, privileged bool,
) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if destination.Spec.External == nil {
		return nil, nil
	}
	provider := destination.Spec.External.Provider

	if !(strings.HasSuffix(provider, rbdProviderName) ||
		strings.HasSuffix(provider, nvmeofProviderName)) {
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
		return nil, fmt.Errorf("missing external parameters in rbd replication destination")
	}

	copyMethod := volsyncv1alpha1.CopyMethodNone
	rawCopyMethod, ok := options["copyMethod"]
	if ok {
		copyMethod = volsyncv1alpha1.CopyMethodType(rawCopyMethod)
	}

	var (
		storageClassName, volumeSnapshotClassName, keySecret *string
		serviceType                                          *corev1.ServiceType
		serviceAnnotations                                   map[string]string
	)

	storageClassNameStr, ok := options["storageClassName"]
	if ok {
		storageClassName = &storageClassNameStr
	}
	volumeSnapshotClassNameStr, ok := options["volumeSnapshotClassName"]
	if ok {
		volumeSnapshotClassName = &volumeSnapshotClassNameStr
	}
	keySecretStr, ok := options["keySecret"]
	if ok {
		keySecret = &keySecretStr
	}
	serviceTypeStr, ok := options["serviceType"]
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
		nil) // No specific SA for rbd mover

	var destPVC = options["destinationPVC"]

	return &Mover{
		client:             client,
		logger:             logger.WithValues("method", "RBD"),
		eventRecorder:      eventRecorder,
		owner:              destination,
		vh:                 vh,
		saHandler:          saHandler,
		containerImage:     rb.getRBDContainerImage(),
		key:                keySecret,
		serviceType:        serviceType,
		serviceAnnotations: serviceAnnotations,
		isSource:           isSource,
		paused:             destination.Spec.Paused,
		mainPVCName:        &destPVC,
		cleanupTempPVC:     false, // Not applicable for rbd
		privileged:         privileged,
		destStatus:         destination.Status.RsyncTLS,
		latestMoverStatus:  destination.Status.LatestMoverStatus,
		moverConfig:        volsyncv1alpha1.MoverConfig{},
		options:            options,
	}, nil
}
