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
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/version/v1"
)

// ConnectToDestination establishes a gRPC connection
// to the destination and verifies it with a GetVersion
// call. It retries the version handshake with
// exponential backoff to handle the case where the
// source starts before the destination's gRPC server
// is ready. Additional dial options (e.g. max message
// size) can be passed via opts.
func ConnectToDestination(
	ctx context.Context,
	logger logr.Logger,
	address string,
	opts ...grpc.DialOption,
) (*grpc.ClientConn, error) {
	dialOpts := make(
		[]grpc.DialOption, 0, 1+len(opts),
	)
	// TLS is handled at the transport layer by stunnel;
	// the gRPC connection runs over the stunnel tunnel.
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

	if err := versionHandshake(ctx, logger, conn, address); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return conn, nil
}

// versionHandshake retries the GetVersion RPC with
// exponential backoff until the destination responds
// or the overall ConnectionTimeout expires.
func versionHandshake(
	ctx context.Context,
	logger logr.Logger,
	conn *grpc.ClientConn,
	address string,
) error {
	versionClient := versionv1.NewVersionServiceClient(conn)

	deadline, cancel := context.WithTimeout(ctx, ConnectionTimeout)
	defer cancel()

	backoff := initialRetryBackoff
	for attempt := 1; ; attempt++ {
		callCtx, callCancel := context.WithTimeout(deadline, rpcRetryTimeout)
		resp, err := versionClient.GetVersion(callCtx, &versionv1.GetVersionRequest{})
		callCancel()

		if err == nil {
			logger.Info("Connected to destination", "version", resp.GetVersion(), "attempt", attempt)
			return nil
		}

		// Check if the overall deadline has expired.
		if deadline.Err() != nil {
			return fmt.Errorf("failed to get version from destination %s after %d attempts: %w", address, attempt, err)
		}

		logger.Info("Destination not ready, retrying", "attempt", attempt, "backoff", backoff, "error", err)

		select {
		case <-time.After(backoff):
		case <-deadline.Done():
			return fmt.Errorf("failed to get version from destination %s after %d attempts: %w", address, attempt, err)
		}

		backoff *= 2
		if backoff > maxRetryBackoff {
			backoff = maxRetryBackoff
		}
	}
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
