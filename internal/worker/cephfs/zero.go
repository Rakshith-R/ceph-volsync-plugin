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

package cephfs

import (
	"os"

	"golang.org/x/sys/unix"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// zeroRange zeroes length bytes at offset using
// FALLOC_FL_PUNCH_HOLE to create a sparse hole. Falls
// back to writing zeros if fallocate is not supported.
func zeroRange(f *os.File, offset, length int64) error {
	err := unix.Fallocate(
		int(f.Fd()), //nolint:gosec // G115: fd fits in int
		unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
		offset, length,
	)
	if err == nil {
		return nil
	}
	return common.WriteZeros(f, offset, length)
}
