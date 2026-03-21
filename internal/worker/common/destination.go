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
	"fmt"
	"net"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/version/v1"
)

// RunDestinationServer starts a gRPC server with the
// given DataServiceServer implementation, plus shared
// VersionService and DoneService. It blocks until the
// context is cancelled, a server error occurs, or the
// DoneService signals shutdown.
func RunDestinationServer(
	ctx context.Context,
	logger logr.Logger,
	serverPort string,
	dataServer apiv1.DataServiceServer,
	opts ...grpc.ServerOption,
) error {
	server := grpc.NewServer(opts...)

	shutdownChan := make(chan struct{}, 1)

	versionServer := &VersionServer{
		Version: "v1.0.0",
	}
	doneServer := &DoneServer{
		ShutdownChan: shutdownChan,
	}

	versionv1.RegisterVersionServiceServer(
		server, versionServer,
	)
	apiv1.RegisterDataServiceServer(
		server, dataServer,
	)
	apiv1.RegisterDoneServiceServer(
		server, doneServer,
	)

	lis, err := net.Listen("tcp", ":"+serverPort)
	if err != nil {
		return fmt.Errorf(
			"failed to listen on port %s: %w",
			serverPort, err,
		)
	}

	logger.Info(
		"gRPC server listening", "port", serverPort,
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
		logger.Info(
			"Destination worker shutting down " +
				"due to context cancellation",
		)
		server.GracefulStop()
		return ctx.Err()
	case err := <-serverErr:
		return err
	case <-shutdownChan:
		logger.Info(
			"Destination worker shutting down " +
				"after Done request",
		)
		server.GracefulStop()
		return nil
	}
}
