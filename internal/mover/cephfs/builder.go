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
	"flag"
	"fmt"

	"github.com/go-logr/logr"
	"github.com/spf13/viper"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	"github.com/backube/volsync/controllers/utils"
	"github.com/backube/volsync/controllers/volumehandler"
)

const (
	cephfsProviderName = "openshift-storage.cephfs.csi.ceph.com"
	cephfsMoverName    = "cephfs"
	// defaultCephFSContainerImage is the default container image for the
	// cephfs data mover
	defaultCephFSContainerImage = "ghcr.io/rakshith-r/ceph-volsync-mover:s0.1"
	// Command line flag will be checked first
	// If command line flag not set, the RELATED_IMAGE_ env var will be used
	cephfsContainerImageFlag   = "mover-image"
	cephfsContainerImageEnvVar = "RELATED_IMAGE_CEPHFS_CONTAINER"
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

	mover.Register(b)
	return nil
}

func newBuilder(viper *viper.Viper, flags *flag.FlagSet) (*Builder, error) {
	b := &Builder{
		viper: viper,
		flags: flags,
	}

	// Set default cephfs container image - will be used if both command line flag and env var are not set
	b.viper.SetDefault(cephfsContainerImageFlag, defaultCephFSContainerImage)

	// Setup command line flag for the cephfs container image
	b.flags.String(cephfsContainerImageFlag, defaultCephFSContainerImage,
		"The container image for the cephfs data mover")
	// Viper will check for command line flag first, then fallback to the env var
	err := b.viper.BindEnv(cephfsContainerImageFlag, cephfsContainerImageEnvVar)

	return b, err
}

func (rb *Builder) Name() string { return cephfsMoverName }

func (rb *Builder) VersionInfo() string {
	return fmt.Sprintf("CephFS container: %s", rb.getCephFSContainerImage())
}

// cephfsContainerImage is the container image name of the cephfs data mover
func (rb *Builder) getCephFSContainerImage() string {
	return rb.viper.GetString(cephfsContainerImageFlag)
}

func (rb *Builder) FromSource(client client.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	source *volsyncv1alpha1.ReplicationSource, privileged bool) (mover.Mover, error) {
	// Only build if the CR belongs to us
	if source.Spec.External == nil || source.Spec.External.Provider != cephfsProviderName {
		return nil, nil
	}

	// Make sure there's a place to write status info
	if source.Status.RsyncTLS == nil {
		source.Status.RsyncTLS = &volsyncv1alpha1.ReplicationSourceRsyncTLSStatus{}
	}

	if source.Status.LatestMoverStatus == nil {
		source.Status.LatestMoverStatus = &volsyncv1alpha1.MoverStatus{}
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(client),
		volumehandler.WithRecorder(eventRecorder),
		volumehandler.WithOwner(source),
		volumehandler.FromSource(&volsyncv1alpha1.ReplicationSourceVolumeOptions{
			CopyMethod: volsyncv1alpha1.CopyMethodNone,
		}),
	)
	if err != nil {
		return nil, err
	}

	isSource := true

	saHandler := utils.NewSAHandler(client, source, isSource, privileged,
		nil) // No specific SA for cephfs mover

	return &Mover{
		client:            client,
		logger:            logger.WithValues("method", "CephFS"),
		eventRecorder:     eventRecorder,
		owner:             source,
		vh:                vh,
		saHandler:         saHandler,
		containerImage:    rb.getCephFSContainerImage(),
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
	if destination.Spec.External == nil || destination.Spec.External.Parameters["mover"] != cephfsMoverName {
		return nil, nil
	}

	// Make sure there's a place to write status info
	if destination.Status.RsyncTLS == nil {
		destination.Status.RsyncTLS = &volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus{}
	}

	if destination.Status.LatestMoverStatus == nil {
		destination.Status.LatestMoverStatus = &volsyncv1alpha1.MoverStatus{}
	}

	vh, err := volumehandler.NewVolumeHandler(
		volumehandler.WithClient(client),
		volumehandler.WithRecorder(eventRecorder),
		volumehandler.WithOwner(destination),
		volumehandler.FromDestination(&volsyncv1alpha1.ReplicationDestinationVolumeOptions{
			CopyMethod: volsyncv1alpha1.CopyMethodNone,
		}),
	)
	if err != nil {
		return nil, err
	}

	isSource := false

	saHandler := utils.NewSAHandler(client, destination, isSource, privileged,
		nil) // No specific SA for cephfs mover

	var destPVC = destination.Spec.External.Parameters["DestinationPVC"]

	return &Mover{
		client:            client,
		logger:            logger.WithValues("method", "CephFS"),
		eventRecorder:     eventRecorder,
		owner:             destination,
		vh:                vh,
		saHandler:         saHandler,
		containerImage:    rb.getCephFSContainerImage(),
		isSource:          isSource,
		paused:            destination.Spec.Paused,
		mainPVCName:       &destPVC,
		cleanupTempPVC:    false, // Not applicable for cephfs
		privileged:        privileged,
		destStatus:        destination.Status.RsyncTLS,
		latestMoverStatus: destination.Status.LatestMoverStatus,
		moverConfig:       volsyncv1alpha1.MoverConfig{},
	}, nil
}
