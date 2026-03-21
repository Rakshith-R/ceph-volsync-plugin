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
