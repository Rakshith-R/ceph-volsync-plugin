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

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
)

func TestDoneSignalsShutdownChan(t *testing.T) {
	s := NewSyncServer(nil, nil, nil, nil)

	if _, err := s.Done(context.Background(), &apiv1.DoneRequest{}); err != nil {
		t.Fatalf("Done() returned error: %v", err)
	}

	select {
	case <-s.shutdownChan:
	default:
		t.Fatal("expected shutdownChan to have a signal after Done()")
	}
}

func TestDoneIdempotent(t *testing.T) {
	s := NewSyncServer(nil, nil, nil, nil)

	for i := 0; i < 3; i++ {
		if _, err := s.Done(context.Background(), &apiv1.DoneRequest{}); err != nil {
			t.Fatalf("Done() call %d returned error: %v", i, err)
		}
	}

	select {
	case <-s.shutdownChan:
	default:
		t.Fatal("expected shutdownChan to have a signal")
	}
}
