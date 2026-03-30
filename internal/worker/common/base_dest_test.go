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
	"testing"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

type mockWriteHandler struct{}

func (m *mockWriteHandler) Write(
	stream grpc.BidiStreamingServer[apiv1.WriteRequest, apiv1.WriteResponse],
) error {
	return nil
}

func TestBaseDestWorker_ListensOnPort(
	t *testing.T,
) {
	w := BaseDestinationWorker{
		Logger: logr.Discard(),
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	errCh := make(chan error, 1)
	go func() {
		syncServer := NewSyncServer(&mockWriteHandler{}, nil, nil, nil)
		errCh <- w.Run(ctx, syncServer)
	}()

	cancel()

	if err := <-errCh; err != nil &&
		err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
}
