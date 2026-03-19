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

package cephfs

import (
	"context"
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

	"github.com/ceph/go-ceph/cephfs"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/config"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/volid"
	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

const (
	// rsync configuration constants
	maxRetries    = 5
	initialDelay  = 2 * time.Second
	backoffFactor = 2
	fileListPath  = "/tmp/filelist.txt"

	// smallFileMaxSize is files this size or smaller
	// that use rsync instead of block diff.
	smallFileMaxSize = 64 * 1024 // 64KB

	// deleteBatchSize is the number of paths per
	// batched delete request.
	deleteBatchSize = 2000

	// rsyncBatchSize is the number of file paths per
	// batched rsync invocation.
	rsyncBatchSize = 2000
)

// syncState holds the context for the sync operation.
type syncState struct {
	differ     *ceph.SnapshotDiffer
	dataClient apiv1.DataServiceClient
	stream     grpc.ClientStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	]
	logger logr.Logger

	deleteChan chan string
	smallChan  chan string
	metaChan   chan string

	rsyncTarget string
}

// SourceWorker represents a CephFS source worker
// instance.
type SourceWorker struct {
	common.BaseSourceWorker
}

// NewSourceWorker creates a new CephFS source
// worker.
func NewSourceWorker(
	logger logr.Logger, cfg common.SourceConfig,
) *SourceWorker {
	return &SourceWorker{
		BaseSourceWorker: common.BaseSourceWorker{
			Logger: logger.WithName(
				"cephfs-source-worker",
			),
			Config: cfg,
		},
	}
}

// Run starts the CephFS source worker.
func (w *SourceWorker) Run(
	ctx context.Context,
) error {
	return w.BaseSourceWorker.Run(ctx, w)
}

// Sync implements common.Syncer. It performs CephFS
// snapdiff sync or falls back to rsync.
func (w *SourceWorker) Sync(
	ctx context.Context,
	conn *grpc.ClientConn,
) error {
	baseSnapshotHandle := os.Getenv(
		worker.EnvBaseSnapshotHandle,
	)
	targetSnapshotHandle := os.Getenv(
		worker.EnvTargetSnapshotHandle,
	)
	volumeHandle := os.Getenv(
		worker.EnvVolumeHandle,
	)

	if baseSnapshotHandle == "" ||
		targetSnapshotHandle == "" ||
		volumeHandle == "" {
		w.Logger.Info(
			"Snapshot handles not set, " +
				"using rsync on /data",
		)
		return w.runRsyncFallback(ctx, conn)
	}

	return w.runSnapdiffSync(
		ctx, conn,
		baseSnapshotHandle,
		targetSnapshotHandle,
		volumeHandle,
	)
}

// runRsyncFallback performs a plain rsync and signals
// completion.
func (w *SourceWorker) runRsyncFallback(
	ctx context.Context, conn *grpc.ClientConn,
) error {
	err := w.rsync()
	if err != nil {
		w.Logger.Error(err, "rsync failed")
		return fmt.Errorf("rsync failed: %w", err)
	}

	return common.SignalDone(ctx, w.Logger, conn)
}

// runSnapdiffSync decodes snapshot handles, creates a
// differ, runs stateless sync, and signals completion.
func (w *SourceWorker) runSnapdiffSync(
	ctx context.Context, conn *grpc.ClientConn,
	baseSnapshotHandle, targetSnapshotHandle,
	volumeHandle string,
) error {
	baseSnapID := &volid.CSIIdentifier{}
	err := baseSnapID.DecomposeCSIID(
		baseSnapshotHandle,
	)
	if err != nil {
		w.Logger.Error(
			err,
			"Failed to decompose BASE_SNAPSHOT_HANDLE",
		)
		return fmt.Errorf(
			"failed to decompose "+
				"BASE_SNAPSHOT_HANDLE: %w",
			err,
		)
	}
	targetSnapID := &volid.CSIIdentifier{}
	err = targetSnapID.DecomposeCSIID(
		targetSnapshotHandle,
	)
	if err != nil {
		w.Logger.Error(
			err,
			"Failed to decompose "+
				"TARGET_SNAPSHOT_HANDLE",
		)
		return fmt.Errorf(
			"failed to decompose "+
				"TARGET_SNAPSHOT_HANDLE: %w",
			err,
		)
	}

	volumeID := &volid.CSIIdentifier{}
	err = volumeID.DecomposeCSIID(volumeHandle)
	if err != nil {
		w.Logger.Error(
			err,
			"Failed to decompose VOLUME_HANDLE",
		)
		return fmt.Errorf(
			"failed to decompose VOLUME_HANDLE: %w",
			err,
		)
	}
	subVolumeGroup, err := config.CephFSSubvolumeGroup(
		config.CsiConfigFile, volumeID.ClusterID,
	)
	if err != nil {
		w.Logger.Error(
			err, "Failed to get subvolume group",
		)
		return fmt.Errorf(
			"failed to get subvolume group: %w", err,
		)
	}
	subVolumeName := "csi-vol-" + volumeID.ObjectUUID
	baseSnapName := "csi-snap-" + baseSnapID.ObjectUUID
	targetSnapName := "csi-snap-" +
		targetSnapID.ObjectUUID

	creds, err := common.ReadMountedCredentials()
	if err != nil {
		return fmt.Errorf(
			"failed to get ceph credentials: %w", err,
		)
	}

	mons, err := config.Mons(
		config.CsiConfigFile, volumeID.ClusterID,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get mons: %w", err,
		)
	}

	dataClient := apiv1.NewDataServiceClient(conn)

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
		return fmt.Errorf(
			"failed to create snapshot differ: %w",
			err,
		)
	}
	defer differ.Destroy()

	if err := w.runStatelessSync(
		ctx, differ, dataClient,
	); err != nil {
		return fmt.Errorf(
			"stateless sync failed: %w", err,
		)
	}

	return common.SignalDone(ctx, w.Logger, conn)
}

// rsync performs the rsync synchronization with retry
// logic.
func (w *SourceWorker) rsync() error {
	startTime := time.Now()
	rsyncDaemonPort := os.Getenv(
		worker.EnvRsyncDaemonPort,
	)
	rsyncTarget := fmt.Sprintf(
		"rsync://127.0.0.1:%s/data", rsyncDaemonPort,
	)

	w.Logger.Info(
		"Starting rsync synchronization",
		"target", rsyncTarget,
	)

	retry := 0
	delay := initialDelay
	rc := 1

	for rc != 0 && retry < maxRetries {
		retry++
		w.Logger.Info(
			"Rsync attempt",
			"retry", retry,
			"maxRetries", maxRetries,
		)

		rcA, err := w.createFileListAndSync(rsyncTarget)
		if err != nil {
			w.Logger.Error(
				err,
				"Failed to create file list or sync",
				"retry", retry,
			)
		}

		if rcA == 0 {
			time.Sleep(1 * time.Second)
		}

		rcB := w.syncForDeletion(rsyncTarget)

		rc = rcA*100 + rcB

		if rc != 0 {
			if retry < maxRetries {
				w.Logger.Info(
					"Synchronization failed, retrying",
					"delay", delay,
					"retry", retry,
					"maxRetries", maxRetries,
					"returnCode", rc,
				)
				time.Sleep(delay)
				delay = delay * backoffFactor
			} else {
				w.Logger.Error(
					fmt.Errorf(
						"rsync failed with code %d",
						rc,
					),
					"Synchronization failed "+
						"after all retries",
				)
			}
		}
	}

	duration := time.Since(startTime)
	w.Logger.Info(
		"Rsync completed",
		"durationSeconds", duration.Seconds(),
		"returnCode", rc,
	)

	if rc != 0 {
		return fmt.Errorf(
			"synchronization failed after %d "+
				"retries, rsync returned: %d",
			maxRetries, rc,
		)
	}

	w.Logger.Info("Synchronization successful")
	return nil
}

// createFileListAndSync generates the file list and
// performs the first rsync pass.
func (w *SourceWorker) createFileListAndSync(
	rsyncTarget string,
) (int, error) {
	findCmd := exec.Command( //nolint:gosec // G204: command args constructed internally
		"sh", "-c",
		fmt.Sprintf(
			"find %s -mindepth 1 -maxdepth 1"+
				" | sed 's|^%s|/|'",
			common.DataMountPath, common.DataMountPath,
		),
	)

	output, err := findCmd.Output()
	if err != nil {
		return 1, fmt.Errorf(
			"failed to list source directory: %w", err,
		)
	}

	if err := os.WriteFile(
		fileListPath, output, 0600,
	); err != nil {
		return 1, fmt.Errorf(
			"failed to write file list: %w", err,
		)
	}

	fileInfo, err := os.Stat(fileListPath)
	if err != nil {
		return 1, fmt.Errorf(
			"failed to stat file list: %w", err,
		)
	}

	if fileInfo.Size() == 0 {
		w.Logger.Info(
			"Skipping sync of empty source directory",
		)
		return 0, nil
	}

	rsyncArgs := []string{
		"-aAhHSxz",
		"-r",
		"--exclude=lost+found",
		"--itemize-changes",
		"--info=stats2,misc2",
		"--files-from=" + fileListPath,
		common.DataMountPath + "/",
		rsyncTarget,
	}

	w.Logger.Info(
		"Running first rsync pass (file preservation)",
		"args", strings.Join(rsyncArgs, " "),
	)

	cmd := exec.Command("rsync", rsyncArgs...) //nolint:gosec // G204: command args constructed internally
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.Logger.Info(
				"First rsync pass failed",
				"exitCode", exitErr.ExitCode(),
			)
			return exitErr.ExitCode(), nil
		}
		w.Logger.Error(
			err,
			"Failed to execute first rsync pass",
		)
		return 1, err
	}

	w.Logger.Info(
		"First rsync pass completed successfully",
	)
	return 0, nil
}

// syncForDeletion performs the second rsync pass to
// delete extra files on destination.
func (w *SourceWorker) syncForDeletion(
	rsyncTarget string,
) int {
	rsyncArgs := []string{
		"-rx",
		"--exclude=lost+found",
		"--ignore-existing",
		"--ignore-non-existing",
		"--delete",
		"--itemize-changes",
		"--info=stats2,misc2",
		common.DataMountPath + "/",
		rsyncTarget,
	}

	w.Logger.Info(
		"Running second rsync pass (deletion)",
		"args", strings.Join(rsyncArgs, " "),
	)

	cmd := exec.Command("rsync", rsyncArgs...) //nolint:gosec // G204: command args constructed internally
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.Logger.Info(
				"Second rsync pass failed",
				"exitCode", exitErr.ExitCode(),
			)
			return exitErr.ExitCode()
		}
		w.Logger.Error(
			err,
			"Failed to execute second rsync pass",
		)
		return 1
	}

	w.Logger.Info(
		"Second rsync pass completed successfully",
	)
	return 0
}

// runStatelessSync implements the stateless sync
// algorithm with directory pre-scan.
func (w *SourceWorker) runStatelessSync(
	ctx context.Context,
	differ *ceph.SnapshotDiffer,
	dataClient apiv1.DataServiceClient,
) error {
	w.Logger.Info("Starting stateless snapshot sync")

	stream, err := dataClient.Sync(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to create sync stream: %w", err,
		)
	}

	state := w.initSyncState(differ, dataClient)
	state.stream = stream

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return w.deleteWorker(gctx, state)
	})
	g.Go(func() error {
		return w.smallRsyncWorker(gctx, state)
	})
	g.Go(func() error {
		return w.metaRsyncWorker(gctx, state)
	})

	g.Go(func() error {
		defer close(state.deleteChan)
		defer close(state.smallChan)
		defer close(state.metaChan)

		dirChan, errChan :=
			w.walkAndStreamDirectories()

		for dirPath := range dirChan {
			select {
			case <-gctx.Done():
				return gctx.Err()
			default:
			}
			if err := w.processDirectorySnapdiff(
				gctx, state, dirPath,
			); err != nil {
				return fmt.Errorf(
					"failed to process "+
						"directory %s: %w",
					dirPath, err,
				)
			}
		}

		if err := <-errChan; err != nil {
			return fmt.Errorf(
				"error walking directories: %w", err,
			)
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		return err
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf(
			"failed to close sync stream: %w", err,
		)
	}

	if err := w.rsyncConvergence(state); err != nil {
		return fmt.Errorf(
			"post-traversal convergence failed: %w",
			err,
		)
	}

	w.Logger.Info(
		"Stateless sync completed successfully",
	)
	return nil
}

// walkAndStreamDirectories walks the /data directory
// tree and sends each directory path through a channel
// for immediate processing.
func (w *SourceWorker) walkAndStreamDirectories() (
	<-chan string, <-chan error,
) {
	dirChan := make(chan string)
	errChan := make(chan error, 1)

	go func() {
		defer close(dirChan)

		w.Logger.Info(
			"Starting directory walk",
			"sourceDir", common.DataMountPath,
		)

		count := 0
		err := filepath.WalkDir(
			common.DataMountPath,
			func(
				path string, d fs.DirEntry,
				walkErr error,
			) error {
				if walkErr != nil {
					w.Logger.Error(
						walkErr,
						"Error accessing path "+
							"during directory walk",
						"path", path,
					)
					return walkErr
				}

				if !d.IsDir() {
					return nil
				}

				relPath, err := filepath.Rel(
					common.DataMountPath, path,
				)
				if err != nil {
					return fmt.Errorf(
						"failed to get relative "+
							"path for %s: %w",
						path, err,
					)
				}

				if relPath == "." {
					relPath = "/"
				} else {
					relPath = "/" + relPath
				}

				dirChan <- relPath
				count++
				return nil
			},
		)

		if err != nil {
			errChan <- fmt.Errorf(
				"failed to walk directory tree: %w",
				err,
			)
			return
		}

		w.Logger.Info(
			"Directory walk completed", "count", count,
		)
		errChan <- nil
	}()

	return dirChan, errChan
}

// initSyncState initializes the sync state with
// channels for parallel processing.
func (w *SourceWorker) initSyncState(
	differ *ceph.SnapshotDiffer,
	dataClient apiv1.DataServiceClient,
) *syncState {
	rsyncDaemonPort := os.Getenv(
		worker.EnvRsyncDaemonPort,
	)
	rsyncTarget := fmt.Sprintf(
		"rsync://127.0.0.1:%s/data", rsyncDaemonPort,
	)

	return &syncState{
		differ:      differ,
		dataClient:  dataClient,
		logger:      w.Logger,
		deleteChan:  make(chan string, 1000),
		smallChan:   make(chan string, 1000),
		metaChan:    make(chan string, 1000),
		rsyncTarget: rsyncTarget,
	}
}

// processDirectorySnapdiff processes snapdiff results
// for a single directory. NOT recursive.
func (w *SourceWorker) processDirectorySnapdiff(
	ctx context.Context, state *syncState,
	dirPath string,
) error {
	w.Logger.V(1).Info(
		"Processing directory snapdiff",
		"path", dirPath,
	)

	iterator, err := state.differ.NewSnapDiffIterator(
		dirPath,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create snap diff iterator "+
				"for %s: %w",
			dirPath, err,
		)
	}
	defer func() { _ = iterator.Close() }()

	for {
		entry, err := iterator.Read()
		if err != nil {
			return fmt.Errorf(
				"failed to read entry: %w", err,
			)
		}
		if entry == nil {
			break
		}
		if err := w.processEntry(
			ctx, state, dirPath, entry,
		); err != nil {
			return err
		}
	}

	return nil
}

// processEntry handles a single snapdiff entry.
func (w *SourceWorker) processEntry(
	ctx context.Context, state *syncState,
	dirPath string, entry *cephfs.SnapDiffEntry,
) error {
	entryName := entry.DirEntry.Name()
	entryPath := filepath.Join(dirPath, entryName)

	if entry.DirEntry.DType() == cephfs.DTypeReg {
		return w.processFile(ctx, state, entryPath)
	}

	fullPath := filepath.Join(
		common.DataMountPath, entryPath,
	)
	if _, err := os.Stat(fullPath); os.IsNotExist(err) {
		select {
		case state.deleteChan <- entryPath:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if entry.DirEntry.DType() == cephfs.DTypeDir {
		return nil
	}

	select {
	case state.smallChan <- entryPath:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// processFile handles file processing.
func (w *SourceWorker) processFile(
	ctx context.Context, state *syncState,
	entryPath string,
) error {
	fullPath := filepath.Join(
		common.DataMountPath, entryPath,
	)

	fileInfo, err := os.Stat(fullPath)
	if os.IsNotExist(err) {
		w.Logger.Info(
			"File deleted, queuing for batch delete",
			"path", entryPath,
		)
		select {
		case state.deleteChan <- entryPath:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	} else if err != nil {
		return fmt.Errorf(
			"failed to stat file %s: %w",
			fullPath, err,
		)
	}

	fileSize := fileInfo.Size()

	if fileSize <= smallFileMaxSize {
		select {
		case state.smallChan <- entryPath:
		case <-ctx.Done():
			return ctx.Err()
		}
		w.Logger.V(1).Info(
			"Queued small file for rsync",
			"path", entryPath, "size", fileSize,
		)
		return nil
	}

	select {
	case state.metaChan <- entryPath:
	case <-ctx.Done():
		return ctx.Err()
	}

	return w.streamBlockDiff(ctx, state, entryPath)
}

// streamBlockDiff uses CephFS block diff to send only
// changed blocks.
//
//nolint:unparam // ctx reserved for future use
func (w *SourceWorker) streamBlockDiff(
	_ context.Context, state *syncState,
	relPath string,
) error {
	blockIterator, err :=
		state.differ.NewBlockDiffIterator(relPath)
	if err != nil {
		return fmt.Errorf(
			"failed to create block diff iterator: %w",
			err,
		)
	}
	defer func() { _ = blockIterator.Close() }()

	fullPath := filepath.Join(
		common.DataMountPath, relPath,
	)
	file, err := os.Open(fullPath) //nolint:gosec // G304: path constructed from validated input
	if err != nil {
		return fmt.Errorf(
			"failed to open source file %s: %w",
			fullPath, err,
		)
	}
	defer func() { _ = file.Close() }()

	var accumulatedBlocks []*apiv1.ChangedBlock
	accumulatedPayloadSize := 0

	for blockIterator.More() {
		changedBlocks, err := blockIterator.Read()
		if err != nil {
			return fmt.Errorf(
				"failed to read block diff: %w", err,
			)
		}

		for _, block := range changedBlocks.ChangedBlocks {
			data := make([]byte, block.Len)
			n, err := file.ReadAt(data, int64(block.Offset)) //nolint:gosec // G115: value within safe range
			if err != nil && err != io.EOF {
				return fmt.Errorf(
					"failed to read block at "+
						"offset %d: %w",
					block.Offset, err,
				)
			}
			data = data[:n]

			isZero := common.IsAllZero(data)

			protoBlock := &apiv1.ChangedBlock{
				Offset: block.Offset,
				Length: block.Len,
				IsZero: isZero,
			}

			if !isZero {
				protoBlock.Data = data
				accumulatedPayloadSize += len(data)
			} else {
				accumulatedPayloadSize += 20
			}

			accumulatedBlocks = append(
				accumulatedBlocks, protoBlock,
			)

			if accumulatedPayloadSize >=
				common.WritePayloadMaxSize {
				if err := common.SendBlockWrite(
					state.stream, relPath,
					accumulatedBlocks,
				); err != nil {
					return err
				}
				accumulatedBlocks = nil
				accumulatedPayloadSize = 0
			}
		}

		if accumulatedPayloadSize >=
			common.WritePayloadMinSize {
			if err := common.SendBlockWrite(
				state.stream, relPath,
				accumulatedBlocks,
			); err != nil {
				return err
			}
			accumulatedBlocks = nil
			accumulatedPayloadSize = 0
		}
	}

	if len(accumulatedBlocks) > 0 {
		if err := common.SendBlockWrite(
			state.stream, relPath,
			accumulatedBlocks,
		); err != nil {
			return err
		}
	}

	if err := state.stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Commit{
			Commit: &apiv1.CommitRequest{
				Path: relPath,
			},
		},
	}); err != nil {
		return fmt.Errorf(
			"failed to send commit for %s: %w",
			relPath, err,
		)
	}

	w.Logger.V(1).Info(
		"Streamed block diff", "path", relPath,
	)
	return nil
}

// deleteWorker reads paths from deleteChan, batches
// them, and sends delete requests via the unary Delete
// RPC.
func (w *SourceWorker) deleteWorker(
	ctx context.Context, state *syncState,
) error {
	var batch []string

	for path := range state.deleteChan {
		batch = append(batch, path)

		if len(batch) >= deleteBatchSize {
			if err := w.sendDeleteBatch(
				ctx, state, batch,
			); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := w.sendDeleteBatch(
			ctx, state, batch,
		); err != nil {
			return err
		}
	}

	w.Logger.Info("Delete worker finished")
	return nil
}

// sendDeleteBatch sends a single batched delete
// request.
func (w *SourceWorker) sendDeleteBatch(
	ctx context.Context, state *syncState,
	paths []string,
) error {
	_, err := state.dataClient.Delete(
		ctx,
		&apiv1.DeleteRequest{Paths: paths},
	)
	if err != nil {
		return fmt.Errorf(
			"failed to send batched delete "+
				"(%d paths): %w",
			len(paths), err,
		)
	}
	return nil
}

// smallRsyncWorker reads paths from smallChan, batches
// them, and runs rsync for content+metadata.
func (w *SourceWorker) smallRsyncWorker(
	_ context.Context, state *syncState,
) error {
	var batch []string

	for path := range state.smallChan {
		batch = append(batch, path)

		if len(batch) >= rsyncBatchSize {
			if err := w.rsyncBatch(
				batch, state.rsyncTarget, true,
			); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := w.rsyncBatch(
			batch, state.rsyncTarget, true,
		); err != nil {
			return err
		}
	}

	w.Logger.Info("Small file rsync worker finished")
	return nil
}

// metaRsyncWorker reads paths from metaChan, batches
// them, and runs rsync for metadata only.
func (w *SourceWorker) metaRsyncWorker(
	_ context.Context, state *syncState,
) error {
	var batch []string

	for path := range state.metaChan {
		batch = append(batch, path)

		if len(batch) >= rsyncBatchSize {
			if err := w.rsyncBatch(
				batch, state.rsyncTarget, false,
			); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		if err := w.rsyncBatch(
			batch, state.rsyncTarget, false,
		); err != nil {
			return err
		}
	}

	w.Logger.Info("Metadata rsync worker finished")
	return nil
}

// rsyncBatch writes paths to a temp file and runs
// rsync against it.
func (w *SourceWorker) rsyncBatch(
	paths []string, target string,
	includeContent bool,
) error {
	tmpFile, err := os.CreateTemp(
		"", "rsync-batch-*.txt",
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create temp file for "+
				"rsync batch: %w",
			err,
		)
	}
	tmpPath := tmpFile.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	for _, p := range paths {
		_, _ = fmt.Fprintln(tmpFile, p)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf(
			"failed to close rsync batch "+
				"temp file: %w",
			err,
		)
	}

	return w.rsyncFromList(
		tmpPath, target, includeContent,
	)
}

// rsyncConvergence performs the final rsync pass for
// directory metadata.
func (w *SourceWorker) rsyncConvergence(
	state *syncState,
) error {
	w.Logger.Info(
		"Rsync convergence: syncing directory metadata",
	)
	rsyncDirArgs := []string{
		"-aAhHSxz",
		"-d",
		"--inplace",
		common.DataMountPath + "/",
		state.rsyncTarget,
	}
	cmd := exec.Command("rsync", rsyncDirArgs...) //nolint:gosec // G204: command args constructed internally
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"failed to rsync directory metadata: %w",
			err,
		)
	}

	return nil
}

// rsyncFromList runs rsync with file list.
func (w *SourceWorker) rsyncFromList(
	listPath, target string, includeContent bool,
) error {
	info, err := os.Stat(listPath)
	if err != nil || info.Size() == 0 {
		w.Logger.Info(
			"Skipping rsync, empty list",
			"list", listPath,
		)
		return nil
	}

	var rsyncArgs []string

	if includeContent {
		rsyncArgs = []string{
			"-aAhHSxz",
			"-r",
			"--files-from=" + listPath,
			common.DataMountPath + "/",
			target,
		}
	} else {
		rsyncArgs = []string{
			"-aAhHSxz",
			"--inplace",
			"--files-from=" + listPath,
			common.DataMountPath + "/",
			target,
		}
	}

	cmd := exec.Command("rsync", rsyncArgs...) //nolint:gosec // G204: command args constructed internally
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"rsync failed for %s: %w",
			listPath, err,
		)
	}

	return nil
}
