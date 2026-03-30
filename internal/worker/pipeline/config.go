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

import (
	"fmt"
	"runtime"
)

// Config holds all tunable pipeline parameters.
type Config struct {
	MaxRawMemoryBytes int64
	MaxWindow         int

	ReadWorkers     int
	HashWorkers     int
	CompressWorkers int
	HashSendWorkers int
	DataSendWorkers int

	HashBatchMaxCount int
	HashBatchMaxBytes int64
	DataBatchMaxCount int
	DataBatchMaxBytes int64

	ChunkSize         int64
	WinPressureThresh float64

	ReadChanBuf     int
	HashChanBuf     int
	MismatchChanBuf int
	CompressChanBuf int
}

const (
	defaultChunkSize   = 4 * 1024 * 1024
	minChunkSize       = 64 * 1024
	maxChunkSize       = 8 * 1024 * 1024
	minMaxWindow       = 8
	maxMaxWindow       = 4096
	minReadWorkers     = 2
	minHashSendWorkers = 1
	maxHashSendWorkers = 8
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
	if c.HashWorkers == 0 {
		ncpu := runtime.NumCPU()
		c.HashWorkers = max(1, ncpu/2)
	}
	if c.CompressWorkers == 0 {
		c.CompressWorkers = 1
	}
	if c.HashSendWorkers == 0 {
		c.HashSendWorkers = 2
	}
	if c.DataSendWorkers == 0 {
		c.DataSendWorkers = 4
	}
	if c.MaxWindow == 0 {
		c.MaxWindow = 64
	}
	if c.MaxRawMemoryBytes == 0 {
		c.MaxRawMemoryBytes = 256 * 1024 * 1024
	}
	if c.WinPressureThresh == 0 {
		c.WinPressureThresh = 0.75
	}
	if c.HashBatchMaxCount == 0 {
		c.HashBatchMaxCount = max(minBatchCount, 16)
	}
	if c.HashBatchMaxBytes == 0 {
		c.HashBatchMaxBytes = int64(c.HashBatchMaxCount) * 40
	}
	if c.DataBatchMaxCount == 0 {
		c.DataBatchMaxCount = 16
	}
	if c.DataBatchMaxBytes == 0 {
		c.DataBatchMaxBytes = 8 * 1024 * 1024
	}
	if c.ReadChanBuf == 0 {
		c.ReadChanBuf = c.ReadWorkers
	}
	if c.HashChanBuf == 0 {
		c.HashChanBuf = c.HashWorkers
	}
	if c.MismatchChanBuf == 0 {
		c.MismatchChanBuf = c.HashWorkers
	}
	if c.CompressChanBuf == 0 {
		c.CompressChanBuf = c.CompressWorkers * 2
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
	if c.HashSendWorkers < minHashSendWorkers || c.HashSendWorkers > maxHashSendWorkers {
		return fmt.Errorf(
			"HashSendWorkers %d outside [%d, %d]",
			c.HashSendWorkers, minHashSendWorkers, maxHashSendWorkers,
		)
	}
	if c.DataSendWorkers < minDataSendWorkers || c.DataSendWorkers > maxDataSendWorkers {
		return fmt.Errorf(
			"DataSendWorkers %d outside [%d, %d]",
			c.DataSendWorkers, minDataSendWorkers, maxDataSendWorkers,
		)
	}
	if c.HashBatchMaxCount < minBatchCount || c.HashBatchMaxCount > maxBatchCount {
		return fmt.Errorf(
			"HashBatchMaxCount %d outside [%d, %d]",
			c.HashBatchMaxCount, minBatchCount, maxBatchCount,
		)
	}
	if c.DataBatchMaxCount < minBatchCount || c.DataBatchMaxCount > maxBatchCount {
		return fmt.Errorf(
			"DataBatchMaxCount %d outside [%d, %d]",
			c.DataBatchMaxCount, minBatchCount, maxBatchCount,
		)
	}
	if c.HashWorkers < 1 {
		return fmt.Errorf("HashWorkers %d < 1", c.HashWorkers)
	}
	if c.CompressWorkers < 1 {
		return fmt.Errorf("CompressWorkers %d < 1", c.CompressWorkers)
	}
	if c.WinPressureThresh < 0.50 || c.WinPressureThresh > 0.90 {
		return fmt.Errorf("WinPressureThresh %.2f outside [0.50, 0.90]", c.WinPressureThresh)
	}
	return nil
}
