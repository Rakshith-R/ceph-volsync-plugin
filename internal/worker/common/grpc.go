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

package common

import (
	"context"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/version/v1"
)

// VersionServer implements the VersionService gRPC
// server shared by all mover types.
type VersionServer struct {
	versionv1.UnimplementedVersionServiceServer
	Version string
}

// GetVersion returns version information.
func (s *VersionServer) GetVersion(
	_ context.Context,
	_ *versionv1.GetVersionRequest,
) (*versionv1.GetVersionResponse, error) {
	return &versionv1.GetVersionResponse{
		Version: s.Version,
	}, nil
}

// DoneServer implements the DoneService gRPC server
// shared by all mover types. It signals graceful
// shutdown via ShutdownChan.
type DoneServer struct {
	apiv1.UnimplementedDoneServiceServer
	ShutdownChan chan struct{}
}

// Done signals completion and triggers graceful
// shutdown.
func (s *DoneServer) Done(
	_ context.Context,
	_ *apiv1.DoneRequest,
) (*apiv1.DoneResponse, error) {
	go func() {
		select {
		case s.ShutdownChan <- struct{}{}:
		default:
		}
	}()

	return &apiv1.DoneResponse{}, nil
}
