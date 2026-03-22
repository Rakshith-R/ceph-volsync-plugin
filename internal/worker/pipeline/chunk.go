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
type ReadChunk struct {
	ReqID     uint64
	FilePath  string
	Offset    int64
	Data      []byte
	TotalSize int64
	Held      held
}

// ZeroChunk represents a zero block detected in StageRead.
// Carries Held with hasWin=true so window is released
// via the ack path (not in StageRead).
type ZeroChunk struct {
	ReqID     uint64
	FilePath  string
	Offset    int64
	Length    int64
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
	TotalSize          int64
	Held               held
}
