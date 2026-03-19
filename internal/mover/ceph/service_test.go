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

package mover

import (
	"testing"
)

func TestEnsureService_Source(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, MoverTypeCephFS)
	ctx := t.Context()

	cont, err := m.ensureServiceAndPublishAddress(ctx)
	if err != nil {
		t.Fatalf("ensureServiceAndPublishAddress() error: %v", err)
	}
	if !cont {
		t.Error("expected true for source mover (no service needed)")
	}
}

func TestEnsureService_WithAddress(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, MoverTypeCephFS)
	addr := "10.0.0.1"
	m.address = &addr
	ctx := t.Context()

	cont, err := m.ensureServiceAndPublishAddress(ctx)
	if err != nil {
		t.Fatalf("ensureServiceAndPublishAddress() error: %v", err)
	}
	if !cont {
		t.Error("expected true when address is provided")
	}
}

func TestUpdateStatusAddress_Sets(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, MoverTypeCephFS)
	addr := "10.0.0.5"
	m.updateStatusAddress(&addr)

	if m.destStatus.Address == nil || *m.destStatus.Address != addr {
		t.Errorf("destStatus.Address = %v, want %q", m.destStatus.Address, addr)
	}
}

func TestUpdateStatusAddress_Nil(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, false, MoverTypeCephFS)
	addr := "10.0.0.5"
	m.updateStatusAddress(&addr)
	m.updateStatusAddress(nil)

	if m.destStatus.Address != nil {
		t.Errorf("destStatus.Address = %v, want nil", m.destStatus.Address)
	}
}
