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

package source

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/version/v1"
)

const (
	// connectionTimeout is the timeout for the first RPC call which establishes the connection.
	// This is longer because it includes DNS resolution, TCP handshake, and TLS negotiation.
	connectionTimeout = 60 * time.Second

	// rpcTimeout is the timeout for subsequent RPC calls after connection is established.
	rpcTimeout = 30 * time.Second
)

// Config holds configuration for the source worker
type Config struct {
	DestinationAddress string
}

// Worker represents a source worker instance
type Worker struct {
	logger logr.Logger
	config Config
}

// NewWorker creates a new source worker
func NewWorker(logger logr.Logger, config Config) *Worker {
	return &Worker{
		logger: logger.WithName("source-worker"),
		config: config,
	}
}

// Run starts the source worker
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("Starting source worker")

	// If destination address is provided, establish gRPC connection and call GetVersion
	if w.config.DestinationAddress != "" {
		w.logger.Info("Connecting to destination", "address", w.config.DestinationAddress)

		// Create gRPC connection (non-blocking, connection established lazily)
		conn, err := grpc.NewClient(
			w.config.DestinationAddress,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("failed to create gRPC client for destination %s: %w", w.config.DestinationAddress, err)
		}
		defer conn.Close()

		// Create version service client
		versionClient := versionv1.NewVersionServiceClient(conn)

		// Call GetVersion with longer timeout to allow for connection establishment
		// The first RPC call will trigger connection establishment, so we need more time
		callCtx, cancel := context.WithTimeout(ctx, connectionTimeout)
		defer cancel()

		w.logger.Info("Calling GetVersion on destination", "establishingConnection", true)

		resp, err := versionClient.GetVersion(callCtx, &versionv1.GetVersionRequest{})
		if err != nil {
			w.logger.Error(err, "Failed to get version from destination",
				"address", w.config.DestinationAddress,
				"hint", "Ensure the destination service is running and accessible")
			return fmt.Errorf("failed to get version from destination %s: %w", w.config.DestinationAddress, err)
		}
		w.logger.Info("Retrieved version from destination", "version", resp.GetVersion())

		// Create done service client
		doneClient := apiv1.NewDoneServiceClient(conn)

		// Call Done to signal completion and request graceful shutdown
		// Connection is already established, so use shorter timeout
		doneCtx, doneCancel := context.WithTimeout(ctx, rpcTimeout)
		defer doneCancel()

		_, err = doneClient.Done(doneCtx, &apiv1.DoneRequest{})
		if err != nil {
			w.logger.Error(err, "Failed to send Done signal to destination")
			return fmt.Errorf("failed to send Done signal to destination: %w", err)
		}
		w.logger.Info("Successfully sent Done signal to destination")

		return nil
	} else {
		w.logger.Info("No destination address provided, running without version checks")
		<-ctx.Done()
		w.logger.Info("Source worker shutting down")
		return ctx.Err()
	}

}
