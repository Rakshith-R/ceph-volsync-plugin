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

// Chunk is emitted by the iterator feeder.
type Chunk struct {
	ReqID     uint64
	FilePath  string
	Offset    int64
	Length    int64
	TotalSize int64
}

// ReadChunk carries raw data from StageRead.
// When IsZero is true, Data is nil and Length holds the block size.
type ReadChunk struct {
	ReqID     uint64
	FilePath  string
	Offset    int64
	Length    int64 // block length; for non-zero == len(Data)
	Data      []byte
	IsZero    bool
	TotalSize int64
	Held      held
}

// HashedChunk adds SHA-256 to a ReadChunk.
type HashedChunk struct {
	ReqID     uint64
	FilePath  string
	Offset    int64
	Length    int64 // original block length
	Data      []byte
	Hash      [32]byte
	IsZero    bool
	TotalSize int64
	Held      held
}

// CompressedChunk holds LZ4-compressed data.
type CompressedChunk struct {
	ReqID              uint64
	FilePath           string
	Offset             int64
	Data               []byte
	Hash               [32]byte
	UncompressedLength int64
	IsRaw              bool // true if incompressible
	IsZero             bool
	TotalSize          int64
	Held               held
}
