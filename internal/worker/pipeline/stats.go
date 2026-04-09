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
	"sync/atomic"
	"time"
)

// Stats tracks per-stage timing and throughput metrics.
type Stats struct {
	ReadCount      atomic.Int64
	ReadBytes      atomic.Int64
	ReadTimeNs     atomic.Int64
	ReadZeroCount  atomic.Int64
	CompressCount  atomic.Int64
	CompressTimeNs atomic.Int64
	CompressBytes  atomic.Int64
	SendCount      atomic.Int64
	SendTimeNs     atomic.Int64
	SendBytes      atomic.Int64
	SendFlushes    atomic.Int64
	AckCount       atomic.Int64
	AckTimeNs      atomic.Int64

	// Wall-clock times set by pipeline.Run
	PipelineStart time.Time
	PipelineEnd   time.Time
}

// Summary returns a formatted multi-line summary of pipeline stats.
func (s *Stats) Summary() string {
	wall := s.PipelineEnd.Sub(s.PipelineStart)
	readMs := float64(s.ReadTimeNs.Load()) / 1e6
	compMs := float64(s.CompressTimeNs.Load()) / 1e6
	sendMs := float64(s.SendTimeNs.Load()) / 1e6
	readMB := float64(s.ReadBytes.Load()) / (1024 * 1024)
	sendMB := float64(s.SendBytes.Load()) / (1024 * 1024)

	return fmt.Sprintf(
		"wall=%.1fs read=%.0fms(%.0fMB,%d chunks,%d zero) compress=%.0fms send=%.0fms(%.0fMB,%d flushes) throughput=%.0fMB/s",
		wall.Seconds(),
		readMs, readMB, s.ReadCount.Load(), s.ReadZeroCount.Load(),
		compMs,
		sendMs, sendMB, s.SendFlushes.Load(),
		readMB/wall.Seconds(),
	)
}
