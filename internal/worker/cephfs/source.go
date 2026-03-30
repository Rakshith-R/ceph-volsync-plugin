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

package cephfs

import (
	"context"
	"fmt"
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

	cephfsdiffer "github.com/RamenDR/ceph-volsync-plugin/internal/ceph/cephfs"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/config"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/volid"
	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
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

	// smallBatchSize is the number of paths per
	// batched rsync request for small files.
	smallBatchSize = 500
)

// setRsyncUDSEnv configures the rsync command to connect
// through a Unix domain socket via socat, bypassing TCP.
func setRsyncUDSEnv(cmd *exec.Cmd) {
	cmd.Env = append(os.Environ(), "RSYNC_CONNECT_PROG=socat - UNIX-CONNECT:/tmp/stunnel/rsync.sock")
}

// syncState holds the context for the sync operation.
type syncState struct {
	differ     *cephfsdiffer.SnapshotDiffer
	syncClient apiv1.SyncServiceClient
	logger     logr.Logger

	rsyncTarget string
}

// cephBlockDiffAdapter bridges *cephfsdiffer.BlockDiffIterator
// to the cephfs-package fileDiffIterator interface.
type cephBlockDiffAdapter struct {
	inner *cephfsdiffer.BlockDiffIterator
}

func (a *cephBlockDiffAdapter) More() bool {
	return a.inner.More()
}

func (a *cephBlockDiffAdapter) Read() (
	[]changedBlock, error,
) {
	cbs, err := a.inner.Read()
	if err != nil || cbs == nil {
		return nil, err
	}
	result := make([]changedBlock, len(cbs.ChangedBlocks))
	for i, b := range cbs.ChangedBlocks {
		result[i] = changedBlock{
			Offset: b.Offset, Len: b.Len,
		}
	}
	return result, nil
}

func (a *cephBlockDiffAdapter) Close() error {
	return a.inner.Close()
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
		constant.EnvBaseSnapshotHandle,
	)
	targetSnapshotHandle := os.Getenv(
		constant.EnvTargetSnapshotHandle,
	)
	volumeHandle := os.Getenv(
		constant.EnvVolumeHandle,
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
	w.Logger.Info("Starting snapdiff sync",
		"baseSnapshotHandle", baseSnapshotHandle,
		"targetSnapshotHandle", targetSnapshotHandle,
		"volumeHandle", volumeHandle,
	)

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

	w.Logger.Info("Decomposed CSI IDs",
		"baseSnapUUID", baseSnapID.ObjectUUID,
		"targetSnapUUID", targetSnapID.ObjectUUID,
		"volumeUUID", volumeID.ObjectUUID,
		"clusterID", volumeID.ClusterID,
		"locationID", volumeID.LocationID,
	)

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

	mons, err := config.Mons(
		config.CsiConfigFile, volumeID.ClusterID,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get mons: %w", err,
		)
	}

	w.Logger.Info("Creating snapshot differ",
		"mons", mons,
		"locationID", volumeID.LocationID,
		"subVolumeGroup", subVolumeGroup,
		"subVolumeName", subVolumeName,
		"baseSnapName", baseSnapName,
		"targetSnapName", targetSnapName,
	)

	syncClient := apiv1.NewSyncServiceClient(conn)

	differ, err := cephfsdiffer.New(
		mons,
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

	w.Logger.Info("Snapshot differ created successfully")

	if err := w.runStatelessSync(
		ctx, conn, differ, syncClient,
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
	rsyncTarget := "rsync://localhost/data"

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
			constant.DataMountPath, constant.DataMountPath,
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
		constant.DataMountPath + "/",
		rsyncTarget,
	}

	w.Logger.Info(
		"Running first rsync pass (file preservation)",
		"args", strings.Join(rsyncArgs, " "),
	)

	cmd := exec.Command("rsync", rsyncArgs...) //nolint:gosec // G204: command args constructed internally
	setRsyncUDSEnv(cmd)
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
		constant.DataMountPath + "/",
		rsyncTarget,
	}

	w.Logger.Info(
		"Running second rsync pass (deletion)",
		"args", strings.Join(rsyncArgs, " "),
	)

	cmd := exec.Command("rsync", rsyncArgs...) //nolint:gosec // G204: command args constructed internally
	setRsyncUDSEnv(cmd)
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
// algorithm with concurrent channel-based streaming.
// A producer goroutine routes entries to category
// channels; consumers process them concurrently in
// an errgroup.
//
//nolint:funlen // errgroup orchestration
func (w *SourceWorker) runStatelessSync(
	ctx context.Context,
	conn *grpc.ClientConn,
	differ *cephfsdiffer.SnapshotDiffer,
	syncClient apiv1.SyncServiceClient,
) error {
	w.Logger.Info("Starting stateless snapshot sync")

	state := w.initSyncState(differ, syncClient)

	g, gctx := errgroup.WithContext(ctx)

	// Producer (in errgroup — errors cancel all)
	largeCh, smallCh, deletedCh :=
		w.streamChangedEntries(gctx, g, state)

	committedCh := make(chan string, 1000)

	// Pipeline consumer
	g.Go(func() error {
		defer close(committedCh)
		return w.runSnapdiffBlockPipeline(
			gctx, differ, conn,
			largeCh, committedCh,
		)
	})

	// Metadata rsync consumer
	g.Go(func() error {
		return w.consumeMetaRsync(
			gctx, committedCh,
			state.rsyncTarget,
		)
	})

	// Small file consumer
	g.Go(func() error {
		return w.consumeSmallFiles(
			gctx, smallCh, state.rsyncTarget,
		)
	})

	// Delete consumer
	g.Go(func() error {
		return w.consumeDeletes(
			gctx, state, deletedCh,
		)
	})

	if err := g.Wait(); err != nil {
		return err
	}

	// Convergence rsync (directory metadata)
	if err := w.rsyncConvergence(state); err != nil {
		return fmt.Errorf(
			"convergence rsync: %w", err,
		)
	}

	w.Logger.Info(
		"Stateless sync completed successfully",
	)
	return nil
}

// streamChangedEntries walks the snapdiff and routes
// entries to category channels as they are discovered.
// The producer goroutine joins the provided errgroup
// so errors cancel all consumers.
func (w *SourceWorker) streamChangedEntries(
	ctx context.Context,
	g *errgroup.Group,
	state *syncState,
) (
	largeCh, smallCh, deletedCh <-chan string,
) {
	lCh := make(chan string, 1000)
	sCh := make(chan string, 1000)
	dCh := make(chan string, 1000)

	g.Go(func() error {
		defer close(lCh)
		defer close(sCh)
		defer close(dCh)

		return w.produceEntries(
			ctx, state, lCh, sCh, dCh,
		)
	})

	return lCh, sCh, dCh
}

// produceEntries walks directories and sends each
// changed entry to the appropriate channel.
//
//nolint:funlen,cyclop // entry categorization logic
func (w *SourceWorker) produceEntries(
	ctx context.Context,
	state *syncState,
	largeCh, smallCh, deletedCh chan<- string,
) error {
	dirChan, errChan := w.walkAndStreamDirectories()

	for dirPath := range dirChan {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := w.categorizeDir(
			ctx, state, dirPath,
			largeCh, smallCh, deletedCh,
		); err != nil {
			return err
		}
	}

	if walkErr := <-errChan; walkErr != nil {
		return walkErr
	}

	return nil
}

// categorizeDir opens a SnapDiffIterator for a single
// directory and routes each entry to the appropriate
// channel.
//
//nolint:cyclop,funlen // entry type routing
func (w *SourceWorker) categorizeDir(
	ctx context.Context,
	state *syncState,
	dirPath string,
	largeCh, smallCh, deletedCh chan<- string,
) error {
	iterator, err :=
		state.differ.NewSnapDiffIterator(dirPath)
	if err != nil {
		return fmt.Errorf(
			"snap diff iterator %s: %w",
			dirPath, err,
		)
	}
	defer func() { _ = iterator.Close() }()

	for {
		entry, readErr := iterator.Read()
		if readErr != nil {
			return readErr
		}
		if entry == nil {
			return nil
		}
		entryName := entry.DirEntry.Name()
		entryPath := filepath.Join(
			dirPath, entryName,
		)

		if entry.DirEntry.DType() != cephfs.DTypeReg {
			fullP := filepath.Join(
				constant.DataMountPath, entryPath,
			)
			if _, statErr := os.Stat(fullP); os.IsNotExist(statErr) {
				select {
				case deletedCh <- entryPath:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else if entry.DirEntry.DType() !=
				cephfs.DTypeDir {
				select {
				case smallCh <- entryPath:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			continue
		}

		fullP := filepath.Join(
			constant.DataMountPath, entryPath,
		)
		fi, statErr := os.Stat(fullP)
		if os.IsNotExist(statErr) {
			select {
			case deletedCh <- entryPath:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		} else if statErr != nil {
			return statErr
		}

		if fi.Size() <= smallFileMaxSize {
			select {
			case smallCh <- entryPath:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			select {
			case largeCh <- entryPath:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// runSnapdiffBlockPipeline runs the 6-stage pipeline
// for large changed files streamed via largeCh.
// Committed file paths are sent to committedCh for
// metadata rsync.
//
//nolint:funlen // pipeline setup with commit drainer
func (w *SourceWorker) runSnapdiffBlockPipeline(
	ctx context.Context,
	differ *cephfsdiffer.SnapshotDiffer,
	conn *grpc.ClientConn,
	largeCh <-chan string,
	committedCh chan<- string,
) error {
	// Peek first element for early exit.
	first, ok := <-largeCh
	if !ok {
		return nil // no large files
	}

	// Proxy channel: re-inject first element.
	proxyCh := make(chan string, 1000)
	proxyCh <- first
	go func() {
		defer close(proxyCh)
		for f := range largeCh {
			select {
			case proxyCh <- f:
			case <-ctx.Done():
				return
			}
		}
	}()

	newIter := func(relPath string) (
		fileDiffIterator, error,
	) {
		it, err := differ.NewBlockDiffIterator(relPath)
		if err != nil {
			return nil, err
		}
		return &cephBlockDiffAdapter{inner: it}, nil
	}

	reader := NewCephFSReader()
	defer func() { _ = reader.Close() }()

	syncClient := apiv1.NewSyncServiceClient(conn)

	newStream := func(
		ctx context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.WriteRequest, apiv1.WriteResponse,
	], error) {
		return syncClient.Write(ctx)
	}
	newHashStream := func(
		ctx context.Context,
	) (grpc.BidiStreamingClient[
		apiv1.HashRequest,
		apiv1.HashResponse,
	], error) {
		return syncClient.CompareHashes(ctx)
	}

	cfg := pipeline.Config{ReadWorkers: 2}
	cfg.SetDefaults()

	win := pipeline.NewWindowSemaphore(cfg.MaxWindow)
	boundaryCh := make(
		chan fileBoundary, cfg.MaxWindow,
	)

	sizeFn := func(path string) (int64, error) {
		full := constant.DataMountPath + "/" + path
		fi, err := os.Stat(full)
		if err != nil {
			return 0, err
		}
		return fi.Size(), nil
	}

	iter := NewCephFSBlockIterator(
		newIter, nil,
		WithFileChan(proxyCh),
		WithContext(ctx),
		WithSizeFunc(sizeFn),
		WithBoundaryChan(boundaryCh),
	)

	commitStream, err := syncClient.Commit(ctx)
	if err != nil {
		_ = iter.Close()
		return fmt.Errorf(
			"open commit stream: %w", err,
		)
	}

	drainer := &commitDrainer{
		boundaryCh:   boundaryCh,
		reader:       reader,
		commitStream: commitStream,
		win:          win,
		maxWindow:    cfg.MaxWindow,
		committedCh:  committedCh,
	}

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return drainer.Run(gctx)
	})

	g.Go(func() error {
		p := pipeline.New(cfg)
		pErr := p.Run(
			gctx, iter, reader,
			newStream, newHashStream, win,
		)
		if pErr != nil {
			iter.SetFailed()
		}
		_ = iter.Close()
		close(boundaryCh)
		return pErr
	})

	return g.Wait()
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
			"sourceDir", constant.DataMountPath,
		)

		count := 0
		err := filepath.WalkDir(
			constant.DataMountPath,
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
					constant.DataMountPath, path,
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

// initSyncState initializes the sync state.
func (w *SourceWorker) initSyncState(
	differ *cephfsdiffer.SnapshotDiffer,
	syncClient apiv1.SyncServiceClient,
) *syncState {
	rsyncTarget := "rsync://localhost/data"

	return &syncState{
		differ:      differ,
		syncClient:  syncClient,
		logger:      w.Logger,
		rsyncTarget: rsyncTarget,
	}
}

// sendDeleteBatch sends a single batched delete
// request on the given stream.
func (w *SourceWorker) sendDeleteBatch(
	stream grpc.BidiStreamingClient[
		apiv1.DeleteRequest, apiv1.DeleteResponse,
	],
	paths []string,
) error {
	if err := stream.Send(
		&apiv1.DeleteRequest{Paths: paths},
	); err != nil {
		return fmt.Errorf(
			"failed to send batched delete "+
				"(%d paths): %w",
			len(paths), err,
		)
	}
	if _, err := stream.Recv(); err != nil {
		return fmt.Errorf(
			"failed to recv delete ack "+
				"(%d paths): %w",
			len(paths), err,
		)
	}
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

// consumeSmallFiles batches paths from smallCh and
// runs rsync for each batch.
func (w *SourceWorker) consumeSmallFiles(
	ctx context.Context,
	smallCh <-chan string,
	target string,
) error {
	batch := make([]string, 0, smallBatchSize)

	for {
		select {
		case p, ok := <-smallCh:
			if !ok {
				return w.flushBatch(
					batch, target, true,
				)
			}
			batch = append(batch, p)
			if len(batch) >= smallBatchSize {
				if err := w.rsyncBatch(
					batch, target, true,
				); err != nil {
					return fmt.Errorf(
						"small file rsync: %w", err,
					)
				}
				batch = batch[:0]
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// consumeDeletes batches paths from deletedCh and
// sends batched delete requests via a bidi stream.
func (w *SourceWorker) consumeDeletes(
	ctx context.Context,
	state *syncState,
	deletedCh <-chan string,
) error {
	stream, err := state.syncClient.Delete(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to open delete stream: %w",
			err,
		)
	}
	defer func() { _ = stream.CloseSend() }()

	batch := make([]string, 0, deleteBatchSize)

	for {
		select {
		case p, ok := <-deletedCh:
			if !ok {
				return w.flushDeletes(
					stream, batch,
				)
			}
			batch = append(batch, p)
			if len(batch) >= deleteBatchSize {
				if err := w.sendDeleteBatch(
					stream, batch,
				); err != nil {
					return err
				}
				batch = batch[:0]
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// consumeMetaRsync batches committed large file paths
// and runs metadata-only rsync for each batch.
func (w *SourceWorker) consumeMetaRsync(
	ctx context.Context,
	committedCh <-chan string,
	target string,
) error {
	batch := make([]string, 0, smallBatchSize)

	for {
		select {
		case p, ok := <-committedCh:
			if !ok {
				return w.flushBatch(
					batch, target, false,
				)
			}
			batch = append(batch, p)
			if len(batch) >= smallBatchSize {
				if err := w.rsyncBatch(
					batch, target, false,
				); err != nil {
					return fmt.Errorf(
						"meta rsync: %w", err,
					)
				}
				batch = batch[:0]
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// flushBatch sends remaining paths via rsync if any.
func (w *SourceWorker) flushBatch(
	batch []string, target string,
	includeContent bool,
) error {
	if len(batch) == 0 {
		return nil
	}
	if err := w.rsyncBatch(
		batch, target, includeContent,
	); err != nil {
		return fmt.Errorf(
			"rsync flush: %w", err,
		)
	}
	return nil
}

// flushDeletes sends remaining delete paths if any.
func (w *SourceWorker) flushDeletes(
	stream grpc.BidiStreamingClient[
		apiv1.DeleteRequest, apiv1.DeleteResponse,
	],
	batch []string,
) error {
	if len(batch) == 0 {
		return nil
	}
	return w.sendDeleteBatch(stream, batch)
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
		constant.DataMountPath + "/",
		state.rsyncTarget,
	}
	cmd := exec.Command("rsync", rsyncDirArgs...) //nolint:gosec // G204: command args constructed internally
	setRsyncUDSEnv(cmd)
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
			constant.DataMountPath + "/",
			target,
		}
	} else {
		rsyncArgs = []string{
			"-aAhHSxz",
			"--inplace",
			"--files-from=" + listPath,
			constant.DataMountPath + "/",
			target,
		}
	}

	cmd := exec.Command("rsync", rsyncArgs...) //nolint:gosec // G204: command args constructed internally
	setRsyncUDSEnv(cmd)
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
