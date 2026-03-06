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
	"fmt"

	"github.com/go-logr/logr"
	"k8s.io/client-go/tools/events"
	ctrlClient "sigs.k8s.io/controller-runtime/pkg/client"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
)

var (
	ErrNoMoverFound        = fmt.Errorf("a replication method must be specified")
	ErrMultipleMoversFound = fmt.Errorf("only one replication method can be supplied")
)

// Catalog is the list of the available Builders for the controller to use when
// attempting to find an appropriate mover to service the RS/RD CR.
var Catalog []Builder

// Register should be called by each mover via an init function to register the
// mover w/ the main VolSync codebase.
func Register(builder Builder) {
	Catalog = append(Catalog, builder)
}

// Builder is used to construct Mover instances for the different data
// mover types.
type Builder interface {
	// FromSource attempts to construct a Mover from the provided
	// ReplicationSource. If the RS does not reference the Builder's mover type,
	// this function should return (nil, nil).
	FromSource(client ctrlClient.Client, logger logr.Logger,
		eventRecorder events.EventRecorder,
		source *volsyncv1alpha1.ReplicationSource, privileged bool) (mover.Mover, error)

	// FromDestination attempts to construct a Mover from the provided
	// ReplicationDestination. If the RS does not reference the Builder's mover
	// type, this function should return (nil, nil).
	FromDestination(client ctrlClient.Client, logger logr.Logger,
		eventRecorder events.EventRecorder,
		destination *volsyncv1alpha1.ReplicationDestination, privileged bool) (mover.Mover, error)

	// The name of the mover this builder is for
	Name() string

	// VersionInfo returns a string describing the version of this mover. In
	// most cases, this is the container image/tag that will be used.
	VersionInfo() string
}

func GetEnabledMoverList() []string {
	enabledMoverNames := []string{} //nolint:prealloc
	for _, builder := range Catalog {
		enabledMoverNames = append(enabledMoverNames, builder.Name())
	}
	return enabledMoverNames
}

func GetDestinationMoverFromCatalog(client ctrlClient.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	destination *volsyncv1alpha1.ReplicationDestination, privileged bool) (mover.Mover, error) {
	var dataMover mover.Mover
	for _, builder := range Catalog {
		candidate, err := builder.FromDestination(client, logger, eventRecorder, destination, privileged)
		if err == nil && candidate != nil {
			if dataMover != nil {
				// Found 2 movers claiming this CR...
				return nil, ErrMultipleMoversFound
			}
			dataMover = candidate
		}
	}
	if dataMover == nil { // No mover matched
		return nil, ErrNoMoverFound
	}
	return dataMover, nil
}

func GetSourceMoverFromCatalog(client ctrlClient.Client, logger logr.Logger,
	eventRecorder events.EventRecorder,
	source *volsyncv1alpha1.ReplicationSource, privileged bool) (mover.Mover, error) {
	var dataMover mover.Mover
	for _, builder := range Catalog {
		candidate, err := builder.FromSource(client, logger, eventRecorder, source, privileged)
		if err == nil && candidate != nil {
			if dataMover != nil {
				// Found 2 movers claiming this CR...
				return nil, ErrMultipleMoversFound
			}
			dataMover = candidate
		}
	}
	if dataMover == nil { // No mover matched
		return nil, ErrNoMoverFound
	}
	return dataMover, nil
}
