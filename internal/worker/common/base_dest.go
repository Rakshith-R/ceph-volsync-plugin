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

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

// BaseDestinationWorker provides shared destination
// worker scaffolding.
type BaseDestinationWorker struct {
	Logger logr.Logger
	Config DestinationConfig
}

// Run starts the gRPC destination server with
// default MaxRecvMsgSize applied.
func (w *BaseDestinationWorker) Run(
	ctx context.Context,
	dataServer apiv1.DataServiceServer,
) error {
	w.Logger.Info("Starting destination worker")

	return RunDestinationServer(
		ctx, w.Logger,
		DefaultServerPort, dataServer,
		grpc.MaxRecvMsgSize(MaxGRPCMessageSize),
	)
}
