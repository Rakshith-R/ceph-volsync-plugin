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

import "fmt"

// Config holds all tunable pipeline parameters.
type Config struct {
	MaxRawMemoryBytes int64
	MaxWindow         int

	ReadWorkers     int
	DataSendWorkers int

	DataBatchMaxCount int
	DataBatchMaxBytes int64

	ChunkSize   int64
	ReadChanBuf int
}

const (
	defaultChunkSize   = 16 * 1024 * 1024
	minChunkSize       = 64 * 1024
	maxChunkSize       = 16 * 1024 * 1024
	minMaxWindow       = 8
	maxMaxWindow       = 4096
	minReadWorkers     = 2
	minDataSendWorkers = 1
	maxDataSendWorkers = 16
	minBatchCount      = 4
	maxBatchCount      = 256
)

// SetDefaults fills zero-valued fields with defaults.
func (c *Config) SetDefaults() {
	if c.ChunkSize == 0 {
		c.ChunkSize = defaultChunkSize
	}
	if c.ReadWorkers == 0 {
		c.ReadWorkers = 8
	}
	if c.DataSendWorkers == 0 {
		c.DataSendWorkers = 8
	}
	if c.MaxWindow == 0 {
		c.MaxWindow = 1024
	}
	if c.MaxRawMemoryBytes == 0 {
		c.MaxRawMemoryBytes = 1024 * 1024 * 1024
	}
	if c.DataBatchMaxCount == 0 {
		c.DataBatchMaxCount = 16
	}
	if c.DataBatchMaxBytes == 0 {
		c.DataBatchMaxBytes = 16 * 1024 * 1024
	}
	if c.ReadChanBuf == 0 {
		c.ReadChanBuf = c.ReadWorkers * 2
	}
}

func (c *Config) validate() error {
	if c.ChunkSize < minChunkSize || c.ChunkSize > maxChunkSize {
		return fmt.Errorf("ChunkSize %d outside [%d, %d]", c.ChunkSize, minChunkSize, maxChunkSize)
	}
	if c.ChunkSize > c.MaxRawMemoryBytes {
		return fmt.Errorf("ChunkSize %d > MaxRawMemoryBytes %d (would deadlock)", c.ChunkSize, c.MaxRawMemoryBytes)
	}
	if c.MaxWindow < minMaxWindow || c.MaxWindow > maxMaxWindow {
		return fmt.Errorf("MaxWindow %d outside [%d, %d]", c.MaxWindow, minMaxWindow, maxMaxWindow)
	}
	if c.ReadWorkers < minReadWorkers {
		return fmt.Errorf("ReadWorkers %d < %d", c.ReadWorkers, minReadWorkers)
	}
	if c.DataSendWorkers < minDataSendWorkers || c.DataSendWorkers > maxDataSendWorkers {
		return fmt.Errorf(
			"DataSendWorkers %d outside [%d, %d]",
			c.DataSendWorkers, minDataSendWorkers, maxDataSendWorkers,
		)
	}
	if c.DataBatchMaxCount < minBatchCount || c.DataBatchMaxCount > maxBatchCount {
		return fmt.Errorf(
			"DataBatchMaxCount %d outside [%d, %d]",
			c.DataBatchMaxCount, minBatchCount, maxBatchCount,
		)
	}
	return nil
}
