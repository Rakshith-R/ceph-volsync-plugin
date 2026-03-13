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

package destination

import (
	"context"
	"fmt"
	"net"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/version/v1"
)

// Config holds configuration for the destination worker.
type Config struct {
	ServerPort string
}

// VersionServer implements the VersionService gRPC server.
type VersionServer struct {
	versionv1.UnimplementedVersionServiceServer
	version string
}

// GetVersion returns version information.
func (s *VersionServer) GetVersion(
	_ context.Context,
	_ *versionv1.GetVersionRequest,
) (*versionv1.GetVersionResponse, error) {
	return &versionv1.GetVersionResponse{
		Version: s.version,
	}, nil
}

// DoneServer implements the DoneService gRPC server.
type DoneServer struct {
	apiv1.UnimplementedDoneServiceServer
	shutdownChan chan struct{}
}

// Done signals completion and triggers graceful shutdown.
func (s *DoneServer) Done(
	_ context.Context, _ *apiv1.DoneRequest,
) (*apiv1.DoneResponse, error) {
	go func() {
		select {
		case s.shutdownChan <- struct{}{}:
		default:
		}
	}()

	return &apiv1.DoneResponse{}, nil
}

// Worker represents a destination worker instance.
type Worker struct {
	logger logr.Logger
	config Config
}

// NewWorker creates a new destination worker.
func NewWorker(
	logger logr.Logger, config Config,
) *Worker {
	return &Worker{
		logger: logger.WithName("destination-worker"),
		config: config,
	}
}

// Run starts the destination worker.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("Starting destination worker")

	server := grpc.NewServer()
	shutdownChan := make(chan struct{}, 1)

	versionServer := &VersionServer{version: "v0.1.0"}
	doneServer := &DoneServer{
		shutdownChan: shutdownChan,
	}

	versionv1.RegisterVersionServiceServer(
		server, versionServer,
	)
	apiv1.RegisterDoneServiceServer(
		server, doneServer,
	)

	lis, err := net.Listen(
		"tcp", ":"+w.config.ServerPort,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to listen on port %s: %w",
			w.config.ServerPort, err,
		)
	}

	w.logger.Info("gRPC server listening",
		"port", w.config.ServerPort,
	)

	serverErr := make(chan error, 1)

	go func() {
		if err := server.Serve(lis); err != nil {
			serverErr <- fmt.Errorf(
				"gRPC server failed: %w", err,
			)
		}
	}()

	select {
	case <-ctx.Done():
		w.logger.Info(
			"Destination worker shutting down " +
				"due to context cancellation",
		)
		server.GracefulStop()
		return ctx.Err()
	case err := <-serverErr:
		return err
	case <-shutdownChan:
		w.logger.Info(
			"Destination worker shutting down " +
				"after Done request",
		)
		server.GracefulStop()
		return nil
	}
}
