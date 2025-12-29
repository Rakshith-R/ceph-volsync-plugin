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

	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/mover/version/v1"
)

// Config holds configuration for the destination worker
type Config struct {
	ServerPort string
}

// VersionServer implements the VersionService gRPC server
type VersionServer struct {
	versionv1.UnimplementedVersionServiceServer
	version string
}

// GetVersion returns version information
func (s *VersionServer) GetVersion(ctx context.Context, req *versionv1.GetVersionRequest) (*versionv1.GetVersionResponse, error) {
	response := &versionv1.GetVersionResponse{
		Version: s.version,
	}
	return response, nil
}

// DoneServer implements the DoneService gRPC server
type DoneServer struct {
	versionv1.UnimplementedDoneServiceServer
	shutdownChan chan struct{}
}

// Done signals completion and triggers graceful shutdown
func (s *DoneServer) Done(ctx context.Context, req *versionv1.DoneRequest) (*versionv1.DoneResponse, error) {
	// Signal shutdown after responding to the request
	go func() {
		select {
		case s.shutdownChan <- struct{}{}:
		default:
			// Channel already has a signal or is closed
		}
	}()

	return &versionv1.DoneResponse{}, nil
}

// Worker represents a destination worker instance
type Worker struct {
	logger logr.Logger
	config Config
}

// NewWorker creates a new destination worker
func NewWorker(logger logr.Logger, config Config) *Worker {
	return &Worker{
		logger: logger.WithName("destination-worker"),
		config: config,
	}
}

// Run starts the destination worker
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("Starting destination worker")

	// Create gRPC server
	server := grpc.NewServer()

	// Create shutdown channel for coordinating graceful shutdown
	shutdownChan := make(chan struct{}, 1)

	// Create version service
	versionServer := &VersionServer{
		version: "v1.0.0", // TODO: Get from build info or config
	}

	// Create done service
	doneServer := &DoneServer{
		shutdownChan: shutdownChan,
	}

	// Register services
	versionv1.RegisterVersionServiceServer(server, versionServer)
	versionv1.RegisterDoneServiceServer(server, doneServer)

	// Setup listener
	lis, err := net.Listen("tcp", ":"+w.config.ServerPort)
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %w", w.config.ServerPort, err)
	}

	w.logger.Info("gRPC server listening", "port", w.config.ServerPort)

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		if err := server.Serve(lis); err != nil {
			serverErr <- fmt.Errorf("gRPC server failed: %w", err)
		}
	}()

	// Wait for context cancellation, server error, or shutdown signal
	select {
	case <-ctx.Done():
		w.logger.Info("Destination worker shutting down due to context cancellation")
		server.GracefulStop()
		return ctx.Err()
	case err := <-serverErr:
		return err
	case <-shutdownChan:
		w.logger.Info("Destination worker shutting down after Done request")
		server.GracefulStop()
		return nil
	}
}
