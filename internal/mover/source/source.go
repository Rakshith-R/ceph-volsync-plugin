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

	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/mover/version/v1"
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

		// Establish gRPC connection
		conn, err := grpc.NewClient(w.config.DestinationAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("failed to connect to destination %s: %w", w.config.DestinationAddress, err)
		}
		defer conn.Close()

		// Create version service client
		client := versionv1.NewVersionServiceClient(conn)

		// Call GetVersion with timeout
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()

		resp, err := client.GetVersion(callCtx, &versionv1.GetVersionRequest{})
		if err != nil {
			w.logger.Error(err, "Failed to get version from destination")
			return fmt.Errorf("failed to get version from destination: %w", err)
		} else {
			w.logger.Info("Retrieved version from destination", "version", resp.GetVersion())
		}

		return nil

		// Continue with periodic version checks every 30 seconds
		// ticker := time.NewTicker(30 * time.Second)
		// defer ticker.Stop()

		// 	for {
		// 		select {
		// 		case <-ctx.Done():
		// 			w.logger.Info("Source worker shutting down")
		// 			return ctx.Err()
		// 		case <-ticker.C:
		// 			callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		// 			resp, err := client.GetVersion(callCtx, &versionv1.GetVersionRequest{})
		// 			cancel()

		// 			if err != nil {
		// 				w.logger.Error(err, "Failed to get version from destination")
		// 			} else {
		// 				w.logger.Info("Version check", "version", resp.GetVersion())
		// 			}
		// 		}
		// 	}
	} else {
		w.logger.Info("No destination address provided, running without version checks")
		<-ctx.Done()
		w.logger.Info("Source worker shutting down")
		return ctx.Err()
	}

}
