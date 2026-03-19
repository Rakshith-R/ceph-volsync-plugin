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

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
)

// Syncer is implemented by mover-specific source
// workers to perform their sync operation.
type Syncer interface {
	Sync(ctx context.Context,
		conn *grpc.ClientConn) error
}

// BaseSourceWorker provides shared source worker
// scaffolding: gRPC connection setup, version
// handshake, and connection cleanup.
type BaseSourceWorker struct {
	Logger logr.Logger
	Config SourceConfig
}

// Run connects to the destination with default
// MaxCallSendMsgSize, delegates to syncer.Sync(),
// and cleans up the connection.
// The syncer is responsible for calling SignalDone
// at the appropriate point in its flow.
func (w *BaseSourceWorker) Run(
	ctx context.Context, syncer Syncer,
) (err error) {
	if w.Config.DestinationAddress == "" {
		w.Logger.Info(
			"No destination address provided, " +
				"waiting for context " +
				"cancellation",
		)
		<-ctx.Done()

		return ctx.Err()
	}

	w.Logger.Info(
		"Connecting to destination",
		"address",
		w.Config.DestinationAddress,
	)

	conn, err := ConnectToDestination(
		ctx, w.Logger,
		w.Config.DestinationAddress,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(
				MaxGRPCMessageSize,
			),
		),
	)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil &&
			err == nil {
			err = fmt.Errorf(
				"close gRPC connection: %w",
				cerr,
			)
		}
	}()

	return syncer.Sync(ctx, conn)
}
