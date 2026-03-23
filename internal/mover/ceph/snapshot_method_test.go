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

package mover

import (
	"testing"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
)

func TestIsSnapshotReady(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		snap *snapv1.VolumeSnapshot
		want bool
	}{
		{
			name: "nil snapshot",
			snap: nil,
			want: false,
		},
		{
			name: "nil status",
			snap: &snapv1.VolumeSnapshot{},
			want: false,
		},
		{
			name: "nil ReadyToUse",
			snap: &snapv1.VolumeSnapshot{
				Status: &snapv1.VolumeSnapshotStatus{},
			},
			want: false,
		},
		{
			name: "ReadyToUse false",
			snap: &snapv1.VolumeSnapshot{
				Status: &snapv1.VolumeSnapshotStatus{
					ReadyToUse: ptrTo(false),
				},
			},
			want: false,
		},
		{
			name: "ReadyToUse true",
			snap: &snapv1.VolumeSnapshot{
				Status: &snapv1.VolumeSnapshotStatus{
					ReadyToUse: ptrTo(true),
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isSnapshotReady(tt.snap); got != tt.want {
				t.Errorf("isSnapshotReady() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSetSnapshotStatus_Nil(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	ctx := t.Context()

	if err := m.setSnapshotStatus(ctx, nil, "current"); err != nil {
		t.Errorf("setSnapshotStatus(nil) returned error: %v", err)
	}
}

func TestSetSnapshotStatus_Updates(t *testing.T) {
	t.Parallel()
	snap := newSnapshot("snap-1", nil)
	m := newTestMover(t, true, constant.MoverCephFS, snap)
	ctx := t.Context()

	if err := m.setSnapshotStatus(ctx, snap, "current"); err != nil {
		t.Fatalf("setSnapshotStatus() error: %v", err)
	}

	// Verify label was set
	updated := &snapv1.VolumeSnapshot{}
	if err := m.client.Get(ctx, client.ObjectKeyFromObject(snap), updated); err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	got := updated.Labels[m.snapStatusLabelKey]
	if got != "current" {
		t.Errorf("label = %q, want %q", got, "current")
	}
}

func TestSetSnapshotStatus_AlreadySet(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	labels := map[string]string{m.snapStatusLabelKey: "current"}
	snap := newSnapshot("snap-1", labels)

	// Re-create mover with the pre-labeled snapshot
	m = newTestMover(t, true, constant.MoverCephFS, snap)
	ctx := t.Context()

	// Should be a no-op (label already set)
	if err := m.setSnapshotStatus(ctx, snap, "current"); err != nil {
		t.Errorf("setSnapshotStatus() error: %v", err)
	}
}

func TestListSnapshotsWithStatus(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	labelKey := m.snapStatusLabelKey

	snap1 := newSnapshot("snap-1",
		map[string]string{labelKey: "current"})
	snap2 := newSnapshot("snap-2",
		map[string]string{labelKey: "previous"})
	snap3 := newSnapshot("snap-3",
		map[string]string{labelKey: "current"})

	m = newTestMover(t, true, constant.MoverCephFS, snap1, snap2, snap3)
	ctx := t.Context()

	// List current snapshots
	current, err := m.listSnapshotsWithStatus(ctx, "current")
	if err != nil {
		t.Fatalf("listSnapshotsWithStatus() error: %v", err)
	}
	if len(current) != 2 {
		t.Errorf("len(current) = %d, want 2", len(current))
	}

	// List previous snapshots
	previous, err := m.listSnapshotsWithStatus(ctx, "previous")
	if err != nil {
		t.Fatalf("listSnapshotsWithStatus() error: %v", err)
	}
	if len(previous) != 1 {
		t.Errorf("len(previous) = %d, want 1", len(previous))
	}

	// List nonexistent status
	none, err := m.listSnapshotsWithStatus(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("listSnapshotsWithStatus() error: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("len(none) = %d, want 0", len(none))
	}
}

func TestFindSnapshotWithStatus(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	labelKey := m.snapStatusLabelKey
	ctx := t.Context()

	// No snapshots — should return nil
	found, err := m.findCurrentSnapshot(ctx)
	if err != nil {
		t.Fatalf("findCurrentSnapshot() error: %v", err)
	}
	if found != nil {
		t.Error("expected nil, got snapshot")
	}

	// With a matching snapshot
	snap := newSnapshot("snap-1",
		map[string]string{labelKey: "current"})
	m = newTestMover(t, true, constant.MoverCephFS, snap)

	found, err = m.findCurrentSnapshot(ctx)
	if err != nil {
		t.Fatalf("findCurrentSnapshot() error: %v", err)
	}
	if found == nil {
		t.Fatal("expected snapshot, got nil")
	}
	if found.Name != "snap-1" {
		t.Errorf("found.Name = %q, want %q", found.Name, "snap-1")
	}
}

func TestTransitionSnapshotStatuses(t *testing.T) {
	t.Parallel()
	m := newTestMover(t, true, constant.MoverCephFS)
	labelKey := m.snapStatusLabelKey

	current := newSnapshot("snap-current",
		map[string]string{labelKey: "current"})
	prev := newSnapshot("snap-prev",
		map[string]string{labelKey: "previous"})

	m = newTestMover(t, true, constant.MoverCephFS, current, prev)
	ctx := t.Context()

	if err := m.transitionSnapshotStatuses(ctx); err != nil {
		t.Fatalf("transitionSnapshotStatuses() error: %v", err)
	}

	// Current should now be previous
	updated := &snapv1.VolumeSnapshot{}
	if err := m.client.Get(ctx, client.ObjectKeyFromObject(current), updated); err != nil {
		t.Fatalf("Get(current) error: %v", err)
	}
	if got := updated.Labels[labelKey]; got != "previous" {
		t.Errorf("current snapshot label = %q, want %q", got, "previous")
	}
}
