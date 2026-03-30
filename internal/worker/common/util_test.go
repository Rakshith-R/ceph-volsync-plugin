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

import "testing"

func TestIsAllZero(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want bool
	}{
		{"nil slice", nil, true},
		{"empty slice", []byte{}, true},
		{"single zero", []byte{0}, true},
		{"single nonzero", []byte{1}, false},
		{"all zeros", make([]byte, 1024), true},
		{"nonzero at start", append([]byte{1}, make([]byte, 1023)...), false},
		{"nonzero at end", append(make([]byte, 1023), 1), false},
		{"nonzero in middle", func() []byte {
			b := make([]byte, 1024)
			b[512] = 0xff
			return b
		}(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsAllZero(tt.data); got != tt.want {
				t.Errorf("IsAllZero() = %v, want %v", got, tt.want)
			}
		})
	}
}
