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

func TestRdHasMover(t *testing.T) {
	testCases := []struct {
		name     string
		rd       *volsyncv1alpha1.ReplicationDestination
		expected bool
	}{
		{
			name: "No mover",
			rd: &volsyncv1alpha1.ReplicationDestination{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rd",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationDestinationSpec{},
			},
			expected: false,
		},
		{
			name: "Rclone mover",
			rd: &volsyncv1alpha1.ReplicationDestination{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rd",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationDestinationSpec{
					Rclone: &volsyncv1alpha1.ReplicationDestinationRcloneSpec{},
				},
			},
			expected: true,
		},
		{
			name: "Restic mover",
			rd: &volsyncv1alpha1.ReplicationDestination{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rd",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationDestinationSpec{
					Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{},
				},
			},
			expected: true,
		},
		{
			name: "Rsync mover",
			rd: &volsyncv1alpha1.ReplicationDestination{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rd",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationDestinationSpec{
					Rsync: &volsyncv1alpha1.ReplicationDestinationRsyncSpec{},
				},
			},
			expected: true,
		},
		{
			name: "RsyncTLS mover",
			rd: &volsyncv1alpha1.ReplicationDestination{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rd",
					Namespace: "test-ns",
				},
				Spec: volsyncv1alpha1.ReplicationDestinationSpec{
					RsyncTLS: &volsyncv1alpha1.ReplicationDestinationRsyncTLSSpec{},
				},
			},
			expected: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, rdHasMover(tc.rd))
		})
	}
}
