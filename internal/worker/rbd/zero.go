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

package rbd

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"

	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// zeroRange zeroes length bytes at offset on a block
// device using the BLKZEROOUT ioctl. Falls back to
// writing zeros if the ioctl is not supported.
func zeroRange(f *os.File, offset, length int64) error {
	r := [2]uint64{uint64(offset), uint64(length)} //nolint:gosec // G115
	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		f.Fd(),
		unix.BLKZEROOUT,
		uintptr(unsafe.Pointer(&r[0])), //nolint:gosec // G103: unsafe pointer required for syscall fallocate
	)
	if errno == 0 {
		return nil
	}
	return common.WriteZeros(f, offset, length)
}
