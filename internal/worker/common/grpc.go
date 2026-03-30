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

package common

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// WriteHandler handles Write bidi stream.
type WriteHandler interface {
	Write(grpc.BidiStreamingServer[apiv1.WriteRequest, apiv1.WriteResponse]) error
}

// DeleteHandler handles Delete bidi stream.
type DeleteHandler interface {
	Delete(grpc.BidiStreamingServer[apiv1.DeleteRequest, apiv1.DeleteResponse]) error
}

// HashHandler handles CompareHashes bidi stream.
type HashHandler interface {
	CompareHashes(grpc.BidiStreamingServer[apiv1.HashRequest, apiv1.HashResponse]) error
}

// CommitHandler handles Commit bidi stream.
type CommitHandler interface {
	Commit(grpc.BidiStreamingServer[apiv1.CommitRequest, apiv1.CommitResponse]) error
}

// SyncServer implements the SyncService gRPC server
// shared by all mover types. It delegates to
// handler interfaces and signals graceful shutdown.
type SyncServer struct {
	apiv1.UnimplementedSyncServiceServer
	shutdownChan chan struct{}
	writeH       WriteHandler
	deleteH      DeleteHandler
	hashH        HashHandler
	commitH      CommitHandler
}

// NewSyncServer creates a SyncServer with the given
// handlers. Handlers can be nil if not needed.
func NewSyncServer(
	writeH WriteHandler,
	deleteH DeleteHandler,
	hashH HashHandler,
	commitH CommitHandler,
) *SyncServer {
	return &SyncServer{
		shutdownChan: make(chan struct{}, 1),
		writeH:       writeH,
		deleteH:      deleteH,
		hashH:        hashH,
		commitH:      commitH,
	}
}

// Write delegates to WriteHandler.
func (s *SyncServer) Write(
	stream grpc.BidiStreamingServer[apiv1.WriteRequest, apiv1.WriteResponse],
) error {
	if s.writeH == nil {
		return status.Error(codes.Unimplemented, "write not configured")
	}
	return s.writeH.Write(stream)
}

// Delete delegates to DeleteHandler.
func (s *SyncServer) Delete(
	stream grpc.BidiStreamingServer[apiv1.DeleteRequest, apiv1.DeleteResponse],
) error {
	if s.deleteH == nil {
		return status.Error(codes.Unimplemented, "delete not configured")
	}
	return s.deleteH.Delete(stream)
}

// CompareHashes delegates to HashHandler.
func (s *SyncServer) CompareHashes(
	stream grpc.BidiStreamingServer[apiv1.HashRequest, apiv1.HashResponse],
) error {
	if s.hashH == nil {
		return status.Error(codes.Unimplemented, "hash not configured")
	}
	return s.hashH.CompareHashes(stream)
}

// Commit delegates to CommitHandler.
func (s *SyncServer) Commit(
	stream grpc.BidiStreamingServer[apiv1.CommitRequest, apiv1.CommitResponse],
) error {
	if s.commitH == nil {
		return status.Error(codes.Unimplemented, "commit not configured")
	}
	return s.commitH.Commit(stream)
}

// Done signals completion and triggers graceful shutdown.
func (s *SyncServer) Done(
	_ context.Context,
	_ *apiv1.DoneRequest,
) (*apiv1.DoneResponse, error) {
	select {
	case s.shutdownChan <- struct{}{}:
	default:
	}
	return &apiv1.DoneResponse{}, nil
}
