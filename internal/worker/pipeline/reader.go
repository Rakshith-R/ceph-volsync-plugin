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

package pipeline

// DataReader abstracts block-level reads.
// Implementations must be safe for concurrent calls
// from multiple goroutines (ReadWorkers > 1).
type DataReader interface {
	// ReadAt reads length bytes from filePath at
	// offset. filePath is ignored for flat devices.
	ReadAt(
		filePath string, offset, length int64,
	) ([]byte, error)
	// CloseFile releases resources held for filePath
	// after its CommitRequest has been sent.
	// No-op for flat block-device readers.
	CloseFile(filePath string) error
}
