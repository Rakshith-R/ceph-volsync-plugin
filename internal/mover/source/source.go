/*
Copyright 2025.

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

package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ceph/go-ceph/cephfs"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/config"
	cephfsmover "github.com/RamenDR/ceph-volsync-plugin/internal/mover/cephfs"
	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/version/v1"
)

const (
	// connectionTimeout is the timeout for the first RPC call which establishes the connection.
	// This is longer because it includes DNS resolution, TCP handshake, and TLS negotiation.
	connectionTimeout = 60 * time.Second

	// rpcTimeout is the timeout for subsequent RPC calls after connection is established.
	rpcTimeout = 30 * time.Second

	// rsync configuration constants
	maxRetries    = 5
	initialDelay  = 2 * time.Second
	backoffFactor = 2
	sourceDir     = "/data"
	fileListPath  = "/tmp/filelist.txt"

	// Sync optimization thresholds
	smallFileMaxSize = 64 * 1024 // 64KB - files this size or smaller use rsync

	// deleteBatchSize is the number of paths per batched delete request.
	// At ~500 bytes/path avg, 2000 paths ≈ 1MB per gRPC message (within 4MB limit).
	// Destination processes paths sequentially via os.RemoveAll; memory stays bounded.
	deleteBatchSize = 2000

	// rsyncBatchSize is the number of file paths per batched rsync invocation.
	// Amortizes rsync's per-invocation overhead (~200ms for fork + connection + handshake).
	// At 2000, overhead drops to ~6% vs ~76% at 100.
	// Client memory: ~1MB batch slice + ~1MB temp file.
	// Destination: rsync daemon file list ≈ 200KB; transfers file-by-file, not buffered.
	rsyncBatchSize = 2000

	// writePayloadMinSize is the minimum accumulated data payload size
	// before sending a WriteRequest over the gRPC stream.
	writePayloadMinSize = 2 * 1024 * 1024 // 2MB

	// writePayloadMaxSize is the maximum accumulated data payload size.
	// Prevents exceeding the 4MB gRPC default server max receive message size.
	writePayloadMaxSize = 3 * 1024 * 1024 // 3MB

	// maxEntryWorkers is the maximum number of concurrent goroutines
	// processing snapdiff entries within a single directory.
	maxEntryWorkers = 8
)

// streamPool is a fixed-size pool of reusable gRPC sync streams.
// Streams are pre-created and shared across file transfers.
// Each stream handles one file at a time: acquire, send writes + commit, release.
type streamPool struct {
	streams chan grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse]
}

// newStreamPool pre-creates size sync streams and returns a pool.
func newStreamPool(ctx context.Context, dataClient apiv1.DataServiceClient, size int) (*streamPool, error) {
	pool := &streamPool{
		streams: make(chan grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse], size),
	}
	for i := 0; i < size; i++ {
		stream, err := dataClient.Sync(ctx)
		if err != nil {
			pool.closeAll()
			return nil, fmt.Errorf("failed to create sync stream %d/%d: %w", i+1, size, err)
		}
		pool.streams <- stream
	}
	return pool, nil
}

// acquire blocks until a stream is available.
func (p *streamPool) acquire() grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse] {
	return <-p.streams
}

// release returns a stream to the pool for reuse.
func (p *streamPool) release(s grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse]) {
	p.streams <- s
}

// closeAll drains and closes all streams in the pool.
func (p *streamPool) closeAll() error {
	close(p.streams)
	var firstErr error
	for s := range p.streams {
		if _, err := s.CloseAndRecv(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// syncState holds the context for the sync operation
type syncState struct {
	differ     *ceph.SnapshotDiffer
	dataClient apiv1.DataServiceClient
	pool       *streamPool
	logger     logr.Logger

	// Channels for parallel processing
	deleteChan chan string // Paths to delete (batched by deleteWorker)
	smallChan  chan string // Files ≤ smallFileMaxSize (rsync content+metadata)
	metaChan   chan string // Files > smallFileMaxSize (rsync metadata only)

	// Rsync target for background workers
	rsyncTarget string
}

// Config holds configuration for the source worker
type Config struct {
	DestinationAddress string
}

// Worker represents a source worker instance
type Worker struct {
	logger logr.Logger
	config Config
}

// NewWorker creates a new source worker
func NewWorker(logger logr.Logger, config Config) *Worker {
	return &Worker{
		logger: logger.WithName("source-worker"),
		config: config,
	}
}

// Run starts the source worker
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("Starting source worker")

	if w.config.DestinationAddress == "" {
		w.logger.Info("No destination address provided, running without version checks")
		<-ctx.Done()
		w.logger.Info("Source worker shutting down")
		return ctx.Err()
	}

	w.logger.Info("Connecting to destination", "address", w.config.DestinationAddress)

	// Create gRPC connection (non-blocking, connection established lazily)
	conn, err := grpc.NewClient(
		w.config.DestinationAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client for destination %s: %w", w.config.DestinationAddress, err)
	}
	defer conn.Close()

	// Create version service client
	versionClient := versionv1.NewVersionServiceClient(conn)

	// Call GetVersion with longer timeout to allow for connection establishment
	// The first RPC call will trigger connection establishment, so we need more time
	callCtx, cancel := context.WithTimeout(ctx, connectionTimeout)
	defer cancel()

	w.logger.Info("Calling GetVersion on destination", "establishingConnection", true)

	resp, err := versionClient.GetVersion(callCtx, &versionv1.GetVersionRequest{})
	if err != nil {
		w.logger.Error(err, "Failed to get version from destination",
			"address", w.config.DestinationAddress,
			"hint", "Ensure the destination service is running and accessible")
		return fmt.Errorf("failed to get version from destination %s: %w", w.config.DestinationAddress, err)
	}
	w.logger.Info("Retrieved version from destination", "version", resp.GetVersion())

	baseSnapshotHandle := os.Getenv("BASE_SNAPSHOT_HANDLE")
	targetSnapshotHandle := os.Getenv("TARGET_SNAPSHOT_HANDLE")
	volumeHandle := os.Getenv("VOLUME_HANDLE")
	if baseSnapshotHandle == "" || targetSnapshotHandle == "" || volumeHandle == "" {
		w.logger.Info("Snapshot handles not set, using rsync on /data")
		// just use rsync on /data
		err := w.rsync()
		if err != nil {
			w.logger.Error(err, "rsync failed")
			return fmt.Errorf("rsync failed: %w", err)
		}

		// Create done service client and signal completion
		doneClient := apiv1.NewDoneServiceClient(conn)
		doneCtx, doneCancel := context.WithTimeout(ctx, rpcTimeout)
		defer doneCancel()

		_, err = doneClient.Done(doneCtx, &apiv1.DoneRequest{})
		if err != nil {
			w.logger.Error(err, "Failed to send Done signal to destination")
			return fmt.Errorf("failed to send Done signal to destination: %w", err)
		}
		w.logger.Info("Successfully sent Done signal to destination")
		return nil
	}
	//decode
	baseSnapID := &ceph.CSIIdentifier{}
	err = baseSnapID.DecomposeCSIID(baseSnapshotHandle)
	if err != nil {
		w.logger.Error(err, "Failed to decompose BASE_SNAPSHOT_HANDLE")
		return fmt.Errorf("failed to decompose BASE_SNAPSHOT_HANDLE: %w", err)
	}
	targetSnapID := &ceph.CSIIdentifier{}
	err = targetSnapID.DecomposeCSIID(targetSnapshotHandle)
	if err != nil {
		w.logger.Error(err, "Failed to decompose TARGET_SNAPSHOT_HANDLE")
		return fmt.Errorf("failed to decompose TARGET_SNAPSHOT_HANDLE: %w", err)
	}

	volumeID := &ceph.CSIIdentifier{}
	err = volumeID.DecomposeCSIID(volumeHandle)
	if err != nil {
		w.logger.Error(err, "Failed to decompose VOLUME_HANDLE")
		return fmt.Errorf("failed to decompose VOLUME_HANDLE: %w", err)
	}
	subVolumeGroup, err := config.CephFSSubvolumeGroup(config.CsiConfigFile, volumeID.ClusterID)
	if err != nil {
		w.logger.Error(err, "Failed to get subvolume group")
		return fmt.Errorf("failed to get subvolume group: %w", err)
	}
	subVolumeName := "csi-vol-" + volumeID.ObjectUUID
	baseSnapName := "csi-snap-" + baseSnapID.ObjectUUID
	targetSnapName := "csi-snap-" + targetSnapID.ObjectUUID

	// Read ceph admin credentials from mounted secret
	creds, err := readMountedCephCredentials()
	if err != nil {
		return fmt.Errorf(
			"failed to get ceph credentials: %w", err,
		)
	}

	mons, err := config.Mons(
		config.CsiConfigFile, volumeID.ClusterID,
	)
	if err != nil {
		return fmt.Errorf("failed to get mons: %w", err)
	}

	// Create gRPC data client
	dataClient := apiv1.NewDataServiceClient(conn)

	// Initialize SnapshotDiffer
	differ, err := ceph.New(
		mons,
		creds,
		volumeID.LocationID,
		subVolumeGroup,
		subVolumeName,
		baseSnapName,
		targetSnapName,
	)
	if err != nil {
		return fmt.Errorf("failed to create snapshot differ: %w", err)
	}
	defer differ.Destroy()

	// Run stateless sync
	if err := w.runStatelessSync(ctx, differ, dataClient); err != nil {
		return fmt.Errorf("stateless sync failed: %w", err)
	}

	// Signal completion
	doneClient := apiv1.NewDoneServiceClient(conn)
	doneCtx, doneCancel := context.WithTimeout(ctx, rpcTimeout)
	defer doneCancel()

	_, err = doneClient.Done(doneCtx, &apiv1.DoneRequest{})
	if err != nil {
		w.logger.Error(err, "Failed to send Done signal to destination")
		return fmt.Errorf("failed to send Done signal to destination: %w", err)
	}
	w.logger.Info("Successfully sent Done signal to destination")

	return nil
}

// rsync performs the rsync synchronization with retry logic
// This implements the bash script logic from the comments above
func (w *Worker) rsync() error {
	startTime := time.Now()
	rsyncDaemonPort := os.Getenv("RSYNC_DAEMON_PORT")
	rsyncTarget := fmt.Sprintf("rsync://127.0.0.1:%s/data", rsyncDaemonPort)

	w.logger.Info("Starting rsync synchronization", "target", rsyncTarget)

	retry := 0
	delay := initialDelay
	rc := 1 // Non-zero to enter loop

	for rc != 0 && retry < maxRetries {
		retry++
		w.logger.Info("Rsync attempt", "retry", retry, "maxRetries", maxRetries)

		// Create file list of all items at root of PVC
		rcA, err := w.createFileListAndSync(rsyncTarget)
		if err != nil {
			w.logger.Error(err, "Failed to create file list or sync", "retry", retry)
		}

		// Small delay between rsync passes to allow the rsync daemon to clean up the connection
		// This prevents "Connection reset by peer" errors when the second pass starts
		// immediately after the first pass completes
		if rcA == 0 {
			time.Sleep(1 * time.Second)
		}

		// Second pass: delete extra files on destination
		rcB := w.syncForDeletion(rsyncTarget)

		// Combine return codes: rc = rc_a * 100 + rc_b
		rc = rcA*100 + rcB

		if rc != 0 {
			if retry < maxRetries {
				w.logger.Info("Synchronization failed, retrying",
					"delay", delay,
					"retry", retry,
					"maxRetries", maxRetries,
					"returnCode", rc)
				time.Sleep(delay)
				delay = delay * backoffFactor
			} else {
				w.logger.Error(fmt.Errorf("rsync failed with code %d", rc),
					"Synchronization failed after all retries")
			}
		}
	}

	duration := time.Since(startTime)
	w.logger.Info("Rsync completed", "durationSeconds", duration.Seconds(), "returnCode", rc)

	if rc != 0 {
		return fmt.Errorf("synchronization failed after %d retries, rsync returned: %d", maxRetries, rc)
	}

	w.logger.Info("Synchronization successful")
	return nil
}

// createFileListAndSync generates the file list and performs the first rsync pass
func (w *Worker) createFileListAndSync(rsyncTarget string) (int, error) {
	// Find all files/dirs at root of pvc, prepend / to each
	// BusyBox find doesn't support -printf, so use find + sed
	// This matches: find "${SOURCE}" -mindepth 1 -maxdepth 1 | sed "s|^${SOURCE}|/|" > /tmp/filelist.txt
	findCmd := exec.Command("sh", "-c", fmt.Sprintf("find %s -mindepth 1 -maxdepth 1 | sed 's|^%s|/|'", sourceDir, sourceDir))

	output, err := findCmd.Output()
	if err != nil {
		return 1, fmt.Errorf("failed to list source directory: %w", err)
	}

	// Write file list to temp file
	if err := os.WriteFile(fileListPath, output, 0644); err != nil {
		return 1, fmt.Errorf("failed to write file list: %w", err)
	}

	// Check if file list is empty
	fileInfo, err := os.Stat(fileListPath)
	if err != nil {
		return 1, fmt.Errorf("failed to stat file list: %w", err)
	}

	if fileInfo.Size() == 0 {
		w.logger.Info("Skipping sync of empty source directory")
		return 0, nil
	}

	// First rsync pass: preserve as much as possible, exclude root directory
	// rsync -aAhHSxz -r --exclude=lost+found --itemize-changes --info=stats2,misc2 --files-from=/tmp/filelist.txt ${SOURCE}/ rsync://127.0.0.1:$STUNNEL_LISTEN_PORT/data
	rsyncArgs := []string{
		"-aAhHSxz",
		"-r",
		"--exclude=lost+found",
		"--itemize-changes",
		"--info=stats2,misc2",
		"--files-from=" + fileListPath,
		sourceDir + "/",
		rsyncTarget,
	}

	w.logger.Info("Running first rsync pass (file preservation)", "args", strings.Join(rsyncArgs, " "))

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.logger.Info("First rsync pass failed", "exitCode", exitErr.ExitCode())
			return exitErr.ExitCode(), nil
		}
		w.logger.Error(err, "Failed to execute first rsync pass")
		return 1, err
	}

	w.logger.Info("First rsync pass completed successfully")
	return 0, nil
}

// syncForDeletion performs the second rsync pass to delete extra files on destination
func (w *Worker) syncForDeletion(rsyncTarget string) int {
	// Second pass: delete files that exist on destination but not on source
	// rsync -rx --exclude=lost+found --ignore-existing --ignore-non-existing --delete --itemize-changes --info=stats2,misc2 ${SOURCE}/ rsync://127.0.0.1:$STUNNEL_LISTEN_PORT/data
	rsyncArgs := []string{
		"-rx",
		"--exclude=lost+found",
		"--ignore-existing",
		"--ignore-non-existing",
		"--delete",
		"--itemize-changes",
		"--info=stats2,misc2",
		sourceDir + "/",
		rsyncTarget,
	}

	w.logger.Info("Running second rsync pass (deletion)", "args", strings.Join(rsyncArgs, " "))

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.logger.Info("Second rsync pass failed", "exitCode", exitErr.ExitCode())
			return exitErr.ExitCode()
		}
		w.logger.Error(err, "Failed to execute second rsync pass")
		return 1
	}

	w.logger.Info("Second rsync pass completed successfully")
	return 0
}

// runStatelessSync implements the stateless sync algorithm with directory pre-scan.
// It launches background goroutines for delete batching and rsync, processing
// them in parallel with the main directory snapdiff walk.
func (w *Worker) runStatelessSync(
	ctx context.Context,
	differ *ceph.SnapshotDiffer,
	dataClient apiv1.DataServiceClient,
) error {
	w.logger.Info("Starting stateless snapshot sync")

	// Pre-create a fixed pool of reusable sync streams
	pool, err := newStreamPool(ctx, dataClient, maxEntryWorkers)
	if err != nil {
		return fmt.Errorf("failed to create stream pool: %w", err)
	}

	// Initialize sync state with dataClient and pool
	state := w.initSyncState(differ, dataClient, pool)

	g, gctx := errgroup.WithContext(ctx)

	// Background worker: batch delete paths and send via unary Delete RPC
	g.Go(func() error { return w.deleteWorker(gctx, state) })

	// Background worker: batch small files and rsync content+metadata
	g.Go(func() error { return w.smallRsyncWorker(gctx, state) })

	// Background worker: batch large file paths and rsync metadata only
	g.Go(func() error { return w.metaRsyncWorker(gctx, state) })

	// Main processing: walk directories and process snapdiff, feeding channels
	g.Go(func() error {
		defer close(state.deleteChan)
		defer close(state.smallChan)
		defer close(state.metaChan)

		dirChan, errChan := w.walkAndStreamDirectories()

		for dirPath := range dirChan {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}
			if err := w.processDirectorySnapdiff(gctx, state, dirPath); err != nil {
				return fmt.Errorf("failed to process directory %s: %w", dirPath, err)
			}
		}

		if err := <-errChan; err != nil {
			return fmt.Errorf("error walking directories: %w", err)
		}

		return nil
	})

	// Wait for all goroutines (main processing + workers) to finish
	if err := g.Wait(); err != nil {
		return err
	}

	// Close all pooled streams
	if err := pool.closeAll(); err != nil {
		return fmt.Errorf("failed to close stream pool: %w", err)
	}

	// Post-traversal convergence: rsync directory metadata only
	// (small files and file metadata are already handled by background workers)
	if err := w.rsyncConvergence(state); err != nil {
		return fmt.Errorf("post-traversal convergence failed: %w", err)
	}

	w.logger.Info("Stateless sync completed successfully")
	return nil
}

// walkAndStreamDirectories walks the /data directory tree and sends each
// directory path through a channel for immediate processing.
func (w *Worker) walkAndStreamDirectories() (<-chan string, <-chan error) {
	dirChan := make(chan string)
	errChan := make(chan error, 1)

	go func() {
		defer close(dirChan)

		w.logger.Info("Starting directory walk", "sourceDir", sourceDir)

		count := 0
		err := filepath.WalkDir(sourceDir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				w.logger.Error(walkErr, "Error accessing path during directory walk", "path", path)
				return walkErr
			}

			if !d.IsDir() {
				return nil
			}

			relPath, err := filepath.Rel(sourceDir, path)
			if err != nil {
				return fmt.Errorf("failed to get relative path for %s: %w", path, err)
			}

			// Root becomes "/", subdirs get leading "/"
			if relPath == "." {
				relPath = "/"
			} else {
				relPath = "/" + relPath
			}

			dirChan <- relPath
			count++
			return nil
		})

		if err != nil {
			errChan <- fmt.Errorf("failed to walk directory tree: %w", err)
			return
		}

		w.logger.Info("Directory walk completed", "count", count)
		errChan <- nil
	}()

	return dirChan, errChan
}

// initSyncState initializes the sync state with channels for parallel processing
func (w *Worker) initSyncState(
	differ *ceph.SnapshotDiffer,
	dataClient apiv1.DataServiceClient,
	pool *streamPool,
) *syncState {
	rsyncDaemonPort := os.Getenv("RSYNC_DAEMON_PORT")
	rsyncTarget := fmt.Sprintf("rsync://127.0.0.1:%s/data", rsyncDaemonPort)

	return &syncState{
		differ:      differ,
		dataClient:  dataClient,
		pool:        pool,
		logger:      w.logger,
		deleteChan:  make(chan string, 1000),
		smallChan:   make(chan string, 1000),
		metaChan:    make(chan string, 1000),
		rsyncTarget: rsyncTarget,
	}
}

// processDirectorySnapdiff processes snapdiff results for a single directory.
// NOT recursive - iteration over directories is driven by the pre-scanned file.
// Entries are processed in parallel using an errgroup limited to maxEntryWorkers.
func (w *Worker) processDirectorySnapdiff(ctx context.Context, state *syncState, dirPath string) error {
	w.logger.V(1).Info("Processing directory snapdiff", "path", dirPath)

	iterator, err := state.differ.NewSnapDiffIterator(dirPath)
	if err != nil {
		return fmt.Errorf("failed to create snap diff iterator for %s: %w", dirPath, err)
	}
	defer iterator.Close()

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxEntryWorkers)

	for {
		entry, err := iterator.Read()
		if err != nil {
			return fmt.Errorf("failed to read entry: %w", err)
		}
		if entry == nil {
			break
		}

		g.Go(func() error {
			return w.processEntry(gctx, state, dirPath, entry)
		})
	}

	return g.Wait()
}

// processEntry handles a single snapdiff entry (directory or file).
func (w *Worker) processEntry(ctx context.Context, state *syncState, dirPath string, entry *cephfs.SnapDiffEntry) error {
	// DirEntry.Name() returns just the filename; combine with dirPath for full relative path
	entryName := entry.DirEntry.Name()
	entryPath := filepath.Join(dirPath, entryName)

	if entry.DirEntry.DType() == cephfs.DTypeReg {
		// File entry
		err := w.processFile(ctx, state, entryPath)
		if err != nil {
			return fmt.Errorf("failed to process file %s: %w", entryPath, err)
		}
	}

	// check if it exists on disk
	fullPath := filepath.Join(sourceDir, entryPath)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		// Directory does not exist - send to delete channel
		select {
		case state.deleteChan <- entryPath:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if entry.DirEntry.DType() == cephfs.DTypeDir {
		// If directory exists, nothing to do.
		return nil
	}

	// Send special files to rsync directly; they cannot
	// be handled by the normal file processing logic.
	select {
	case state.smallChan <- entryPath:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// processFile handles file processing
func (w *Worker) processFile(ctx context.Context, state *syncState, entryPath string) error {
	fullPath := filepath.Join(sourceDir, entryPath)

	// Check if file was deleted (doesn't exist on disk)
	fileInfo, err := os.Stat(fullPath)
	if os.IsNotExist(err) {
		w.logger.Info("File deleted, queuing for batch delete", "path", entryPath)
		select {
		case state.deleteChan <- entryPath:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to stat file %s: %w", fullPath, err)
	}

	fileSize := fileInfo.Size()

	if fileSize <= smallFileMaxSize {
		// Small file: send to channel for background rsync
		select {
		case state.smallChan <- entryPath:
		case <-ctx.Done():
			return ctx.Err()
		}
		w.logger.V(1).Info("Queued small file for rsync", "path", entryPath, "size", fileSize)
		return nil
	}

	// Large file: send to channel for background metadata rsync
	select {
	case state.metaChan <- entryPath:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Always use block diff for files larger than smallFileMaxSize
	return w.streamBlockDiff(ctx, state, entryPath)
}

// streamBlockDiff uses CephFS block diff to send only changed blocks.
// It acquires a reusable stream from the pool, sends all write requests
// for the file, sends a commit, then releases the stream back to the pool.
func (w *Worker) streamBlockDiff(ctx context.Context, state *syncState, relPath string) error {
	blockIterator, err := state.differ.NewBlockDiffIterator(relPath)
	if err != nil {
		return fmt.Errorf("failed to create block diff iterator: %w", err)
	}
	defer blockIterator.Close()

	// Open source file to read block data
	fullPath := filepath.Join(sourceDir, relPath)
	file, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("failed to open source file %s: %w", fullPath, err)
	}
	defer file.Close()

	// Acquire a reusable stream from the pool
	stream := state.pool.acquire()
	defer state.pool.release(stream)

	// Accumulator for batching blocks across iterator reads
	var accumulatedBlocks []*apiv1.ChangedBlock
	accumulatedPayloadSize := 0

	for blockIterator.More() {
		changedBlocks, err := blockIterator.Read()
		if err != nil {
			return fmt.Errorf("failed to read block diff: %w", err)
		}

		// Convert CephFS blocks to proto blocks and read data
		for _, block := range changedBlocks.ChangedBlocks {
			// Read block data from file at offset
			data := make([]byte, block.Len)
			n, err := file.ReadAt(data, int64(block.Offset))
			if err != nil && err != io.EOF {
				return fmt.Errorf("failed to read block at offset %d: %w", block.Offset, err)
			}
			data = data[:n]

			// Check if block is all zeros (optimization)
			isZero := isAllZero(data)

			protoBlock := &apiv1.ChangedBlock{
				Offset: block.Offset,
				Length: block.Len,
				IsZero: isZero,
			}

			if !isZero {
				protoBlock.Data = data
				accumulatedPayloadSize += len(data)
			} else {
				// Account for proto serialization overhead of zero blocks
				// (~20 bytes for offset/length/is_zero varint fields)
				accumulatedPayloadSize += 20
			}

			accumulatedBlocks = append(accumulatedBlocks, protoBlock)

			// Flush if we hit the max payload size (safety cap for gRPC limit)
			if accumulatedPayloadSize >= writePayloadMaxSize {
				if err := sendWrite(stream, relPath, accumulatedBlocks); err != nil {
					return err
				}
				accumulatedBlocks = nil
				accumulatedPayloadSize = 0
			}
		}

		// Flush if we have reached the minimum payload threshold
		if accumulatedPayloadSize >= writePayloadMinSize {
			if err := sendWrite(stream, relPath, accumulatedBlocks); err != nil {
				return err
			}
			accumulatedBlocks = nil
			accumulatedPayloadSize = 0
		}
	}

	// Flush any remaining accumulated blocks
	if len(accumulatedBlocks) > 0 {
		if err := sendWrite(stream, relPath, accumulatedBlocks); err != nil {
			return err
		}
	}

	// Send CommitRequest to signal file is done (stream can be reused for next file)
	if err := stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Commit{
			Commit: &apiv1.CommitRequest{Path: relPath},
		},
	}); err != nil {
		return fmt.Errorf("failed to send commit for %s: %w", relPath, err)
	}

	w.logger.V(1).Info("Streamed block diff", "path", relPath)
	return nil
}

// sendWrite sends a batch of accumulated blocks as a single WriteRequest on the given stream.
func sendWrite(
	stream grpc.ClientStreamingClient[apiv1.SyncRequest, apiv1.SyncResponse],
	relPath string,
	blocks []*apiv1.ChangedBlock,
) error {
	if err := stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Write{
			Write: &apiv1.WriteRequest{
				Path:   relPath,
				Blocks: blocks,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send write blocks for %s: %w", relPath, err)
	}
	return nil
}

// isAllZero checks if a byte slice contains only zeros
func isAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

// deleteWorker reads paths from deleteChan, batches them, and sends
// delete requests via the unary Delete RPC in parallel with main processing.
func (w *Worker) deleteWorker(ctx context.Context, state *syncState) error {
	var batch []string

	for path := range state.deleteChan {
		batch = append(batch, path)

		if len(batch) >= deleteBatchSize {
			if err := w.sendDeleteBatch(ctx, state, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Send remaining paths
	if len(batch) > 0 {
		if err := w.sendDeleteBatch(ctx, state, batch); err != nil {
			return err
		}
	}

	w.logger.Info("Delete worker finished")
	return nil
}

// sendDeleteBatch sends a single batched delete request via the unary Delete RPC.
func (w *Worker) sendDeleteBatch(ctx context.Context, state *syncState, paths []string) error {
	_, err := state.dataClient.Delete(ctx, &apiv1.DeleteRequest{Paths: paths})
	if err != nil {
		return fmt.Errorf("failed to send batched delete (%d paths): %w", len(paths), err)
	}
	return nil
}

// smallRsyncWorker reads paths from smallChan, batches them, and runs
// rsync for content+metadata in parallel with main processing.
func (w *Worker) smallRsyncWorker(_ context.Context, state *syncState) error {
	var batch []string

	for path := range state.smallChan {
		batch = append(batch, path)

		if len(batch) >= rsyncBatchSize {
			if err := w.rsyncBatch(batch, state.rsyncTarget, true); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Rsync remaining paths
	if len(batch) > 0 {
		if err := w.rsyncBatch(batch, state.rsyncTarget, true); err != nil {
			return err
		}
	}

	w.logger.Info("Small file rsync worker finished")
	return nil
}

// metaRsyncWorker reads paths from metaChan, batches them, and runs
// rsync for metadata only in parallel with main processing.
func (w *Worker) metaRsyncWorker(_ context.Context, state *syncState) error {
	var batch []string

	for path := range state.metaChan {
		batch = append(batch, path)

		if len(batch) >= rsyncBatchSize {
			if err := w.rsyncBatch(batch, state.rsyncTarget, false); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	// Rsync remaining paths
	if len(batch) > 0 {
		if err := w.rsyncBatch(batch, state.rsyncTarget, false); err != nil {
			return err
		}
	}

	w.logger.Info("Metadata rsync worker finished")
	return nil
}

// rsyncBatch writes paths to a temp file and runs rsync against it.
func (w *Worker) rsyncBatch(paths []string, target string, includeContent bool) error {
	tmpFile, err := os.CreateTemp("", "rsync-batch-*.txt")
	if err != nil {
		return fmt.Errorf("failed to create temp file for rsync batch: %w", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	for _, p := range paths {
		fmt.Fprintln(tmpFile, p)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close rsync batch temp file: %w", err)
	}

	return w.rsyncFromList(tmpPath, target, includeContent)
}

// rsyncConvergence performs the final rsync pass for directory metadata.
// Small files and file metadata are already handled by background workers.
func (w *Worker) rsyncConvergence(state *syncState) error {
	w.logger.Info("Rsync convergence: syncing directory metadata")
	rsyncDirArgs := []string{
		"-aAhHSxz",
		"-d",
		"--inplace",
		sourceDir + "/",
		state.rsyncTarget,
	}
	cmd := exec.Command("rsync", rsyncDirArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to rsync directory metadata: %w", err)
	}

	return nil
}

// rsyncFromList runs rsync with file list
func (w *Worker) rsyncFromList(listPath, target string, includeContent bool) error {
	// Check if list file exists and is non-empty
	info, err := os.Stat(listPath)
	if err != nil || info.Size() == 0 {
		w.logger.Info("Skipping rsync, empty list", "list", listPath)
		return nil
	}

	var rsyncArgs []string

	if includeContent {
		// Content + metadata
		rsyncArgs = []string{
			"-aAhHSxz",
			"-r",
			"--files-from=" + listPath,
			sourceDir + "/",
			target,
		}
	} else {
		// Metadata + size fix (content already transferred via block diff)
		// Uses --inplace to avoid creating temp files for large files
		rsyncArgs = []string{
			"-aAhHSxz",
			"--inplace",
			"--files-from=" + listPath,
			sourceDir + "/",
			target,
		}
	}

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("rsync failed for %s: %w", listPath, err)
	}

	return nil
}

// readMountedCephCredentials reads ceph admin credentials
// from a JSON file mounted at
// /etc/ceph-csi-secret/credentials.json.
func readMountedCephCredentials() (
	*ceph.Credentials, error,
) {
	path := filepath.Join(
		cephfsmover.CsiSecretMountPath,
		cephfsmover.CsiSecretJSONKey,
	)
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to read %s: %w", path, err,
		)
	}

	data := map[string]string{}
	if err := json.Unmarshal(content, &data); err != nil {
		return nil, fmt.Errorf(
			"failed to parse %s: %w", path, err,
		)
	}

	return ceph.NewAdminCredentials(data)
}
