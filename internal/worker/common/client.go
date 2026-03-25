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
	"io"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/version/v1"
)

// ConnectToDestination establishes a gRPC connection
// to the destination and verifies it with a GetVersion
// call. Additional dial options (e.g. max message size)
// can be passed via opts.
func ConnectToDestination(
	ctx context.Context,
	logger logr.Logger,
	address string,
	opts ...grpc.DialOption,
) (*grpc.ClientConn, error) {
	dialOpts := make(
		[]grpc.DialOption, 0, 1+len(opts),
	)
	dialOpts = append(dialOpts,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	dialOpts = append(dialOpts, opts...)

	conn, err := grpc.NewClient(address, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create gRPC client for %s: %w",
			address, err,
		)
	}

	versionClient := versionv1.NewVersionServiceClient(
		conn,
	)
	callCtx, cancel := context.WithTimeout(
		ctx, ConnectionTimeout,
	)
	defer cancel()

	resp, err := versionClient.GetVersion(
		callCtx, &versionv1.GetVersionRequest{},
	)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf(
			"failed to get version from "+
				"destination %s: %w",
			address, err,
		)
	}
	logger.Info(
		"Connected to destination",
		"version", resp.GetVersion(),
	)

	return conn, nil
}

// SignalDone sends the Done RPC to the destination,
// signalling that the source has finished.
func SignalDone(
	ctx context.Context,
	logger logr.Logger,
	conn *grpc.ClientConn,
) error {
	syncClient := apiv1.NewSyncServiceClient(conn)
	doneCtx, doneCancel := context.WithTimeout(ctx, RPCTimeout)
	defer doneCancel()

	if _, err := syncClient.Done(doneCtx, &apiv1.DoneRequest{}); err != nil {
		return fmt.Errorf("failed to send Done signal: %w", err)
	}

	logger.Info("Successfully sent Done signal")
	return nil
}

// SendBlockWrite sends a batch of accumulated blocks
// as a single WriteRequest on the given stream. Handles
// EOF errors by checking the server response.
func SendBlockWrite(
	stream grpc.ClientStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	],
	path string,
	blocks []*apiv1.ChangedBlock,
) error {
	if err := stream.Send(&apiv1.WriteRequest{
		Path:   path,
		Blocks: blocks,
	}); err != nil {
		if err == io.EOF {
			if _, recvErr := stream.CloseAndRecv(); recvErr != nil {
				return fmt.Errorf(
					"destination error during "+
						"write for %s: %w",
					path, recvErr,
				)
			}
		}
		return fmt.Errorf(
			"failed to send write blocks for %s: %w",
			path, err,
		)
	}
	return nil
}
