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

package common_test

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/go-logr/logr"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

type mockDataServer struct {
	apiv1.UnimplementedDataServiceServer
}

func TestBaseDestWorker_ListensOnPort(
	t *testing.T,
) {
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := lis.Addr().(*net.TCPAddr).Port
	_ = lis.Close()

	w := common.BaseDestinationWorker{
		Logger: logr.Discard(),
		Config: common.DestinationConfig{
			ServerPort: fmt.Sprintf("%d", port),
		},
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Run(ctx, &mockDataServer{})
	}()

	cancel()

	if err := <-errCh; err != nil &&
		err != context.Canceled {
		t.Fatalf("unexpected error: %v", err)
	}
}
