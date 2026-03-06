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

package controller

import (
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestRsHasMover(t *testing.T) {
	testCases := []struct {
		name     string
		rs       *volsyncv1alpha1.ReplicationSource
		expected bool
	}{
		{
			name: "No mover",
			rs: &volsyncv1alpha1.ReplicationSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rs",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationSourceSpec{},
			},
			expected: false,
		},
		{
			name: "Rclone mover",
			rs: &volsyncv1alpha1.ReplicationSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rs",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationSourceSpec{
					Rclone: &volsyncv1alpha1.ReplicationSourceRcloneSpec{},
				},
			},
			expected: true,
		},
		{
			name: "Restic mover",
			rs: &volsyncv1alpha1.ReplicationSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rs",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationSourceSpec{
					Restic: &volsyncv1alpha1.ReplicationSourceResticSpec{},
				},
			},
			expected: true,
		},
		{
			name: "Rsync mover",
			rs: &volsyncv1alpha1.ReplicationSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rs",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationSourceSpec{
					Rsync: &volsyncv1alpha1.ReplicationSourceRsyncSpec{},
				},
			},
			expected: true,
		},
		{
			name: "RsyncTLS mover",
			rs: &volsyncv1alpha1.ReplicationSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rs",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationSourceSpec{
					RsyncTLS: &volsyncv1alpha1.ReplicationSourceRsyncTLSSpec{},
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, rsHasMover(tc.rs))
		})
	}
}
