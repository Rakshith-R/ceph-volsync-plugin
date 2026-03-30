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
)

type mockSyncer struct {
	called bool
	err    error
}

func (m *mockSyncer) Sync(
	_ context.Context, _ *grpc.ClientConn,
) error {
	m.called = true
	return m.err
}

func TestBaseSourceWorker_EmptyAddress(t *testing.T) {
	w := BaseSourceWorker{
		Logger: logr.Discard(),
		Config: SourceConfig{},
	}
	syncer := &mockSyncer{}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel() // cancel immediately

	err := w.Run(ctx, syncer)
	if err == nil {
		t.Fatal("expected context error")
	}
	if syncer.called {
		t.Fatal(
			"Sync should not be called " +
				"with empty address",
		)
	}
}
