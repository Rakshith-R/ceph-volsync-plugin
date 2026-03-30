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

package rbd

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/config"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/connection"
	cephrbd "github.com/RamenDR/ceph-volsync-plugin/internal/ceph/rbd"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/volid"
	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/pipeline"
)

// SourceWorker represents an RBD source worker
// instance.
type SourceWorker struct {
	common.BaseSourceWorker
}

// NewSourceWorker creates a new RBD source worker.
func NewSourceWorker(
	logger logr.Logger, cfg common.SourceConfig,
) *SourceWorker {
	return &SourceWorker{
		BaseSourceWorker: common.BaseSourceWorker{
			Logger: logger.WithName("rbd-source-worker"),
			Config: cfg,
		},
	}
}

// sourceContext holds resolved state needed for the
// sync operation.
type sourceContext struct {
	mons            string
	radosNS         string
	volumeID        *volid.CSIIdentifier
	parentPoolID    int64
	parentNS        string
	parentImageName string
	parentPoolName  string
	fromSnapID      uint64
	targetSnapID    uint64
}

// Run starts the RBD source worker.
func (w *SourceWorker) Run(ctx context.Context) error {
	return w.BaseSourceWorker.Run(ctx, w)
}

// Sync performs the RBD source sync operation.
//
//nolint:funlen // sequential orchestration steps
func (w *SourceWorker) Sync(
	ctx context.Context, conn *grpc.ClientConn,
) (err error) {
	w.Logger.Info("Starting RBD source sync")

	sc, cc, err := w.resolveSourceConfig()
	if err != nil {
		return err
	}

	if err := w.resolveParentImage(cc, sc); err != nil {
		cc.Destroy()
		return err
	}

	parentSpec := cephrbd.RBDImageSpec(
		sc.parentPoolName, sc.parentNS,
		sc.parentImageName,
	)
	parentImage, err := cephrbd.NewImage(cc, parentSpec)
	if err != nil {
		cc.Destroy()
		return fmt.Errorf(
			"failed to open parent image %s: %w",
			parentSpec, err,
		)
	}

	volSize, err := parentImage.GetSize()
	if err != nil {
		_ = parentImage.Close()
		cc.Destroy()
		return fmt.Errorf(
			"failed to get volume size: %w", err,
		)
	}
	_ = parentImage.Close()
	cc.Destroy()

	iter, err := cephrbd.NewRBDBlockDiffIterator(
		sc.mons,
		sc.parentPoolID, sc.parentNS,
		sc.parentImageName,
		sc.fromSnapID, sc.targetSnapID, volSize,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create block diff iterator: %w",
			err,
		)
	}
	defer func() { _ = iter.Close() }()

	device, err := os.Open(constant.DevicePath)
	if err != nil {
		return fmt.Errorf(
			"failed to open %s: %w",
			constant.DevicePath, err,
		)
	}
	defer func() { _ = device.Close() }()

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

	cfg := pipeline.Config{}
	cfg.SetDefaults()

	win := pipeline.NewWindowSemaphore(cfg.MaxWindow)
	adapter := &rbdIterAdapter{
		iter:      iter,
		totalSize: int64(volSize), //nolint:gosec // G115: volume size within range
	}

	p := pipeline.New(cfg)
	reader := &fileDataReader{file: device}
	if err := p.Run(
		ctx, adapter, reader,
		newStream, newHashStream, win,
	); err != nil {
		return err
	}

	w.Logger.Info("Pipeline completed",
		"blocksProcessed", adapter.reqID,
	)

	// Wait for all acks before committing
	lastReqID := adapter.reqID
	if lastReqID == 0 {
		w.Logger.Info(
			"No changed blocks in diff," +
				" skipping commit",
		)
		return w.closeAndSignalDone(ctx, conn)
	}
	lastReqID--
	for !win.IsReleased(lastReqID) {
		runtime.Gosched()
		time.Sleep(100 * time.Microsecond)
		if ctx.Err() != nil {
			return fmt.Errorf(
				"waiting for acks: %w",
				ctx.Err(),
			)
		}
	}

	// Send commit for the device
	commitStream, err := syncClient.Commit(ctx)
	if err != nil {
		return fmt.Errorf("open commit stream: %w", err)
	}
	if err := commitStream.Send(&apiv1.CommitRequest{
		Entries: []*apiv1.CommitEntry{{
			Path:      constant.DevicePath,
			TotalSize: volSize,
		}},
	}); err != nil {
		return fmt.Errorf("send commit: %w", err)
	}
	if _, err := commitStream.Recv(); err != nil {
		return fmt.Errorf("recv commit ack: %w", err)
	}
	_ = commitStream.CloseSend()

	return w.closeAndSignalDone(ctx, conn)
}

// resolveSourceConfig reads environment variables,
// credentials, and Ceph cluster configuration to
// populate a sourceContext. Returns the sourceContext
// and an active ClusterConnection that the caller must
// destroy.
func (w *SourceWorker) resolveSourceConfig() (
	*sourceContext, *connection.ClusterConnection, error,
) {
	volumeHandle := os.Getenv("VOLUME_HANDLE")

	volumeID := &volid.CSIIdentifier{}
	if err := volumeID.DecomposeCSIID(
		volumeHandle,
	); err != nil {
		return nil, nil, fmt.Errorf(
			"failed to decompose VOLUME_HANDLE: %w",
			err,
		)
	}

	mons, err := config.Mons(
		config.CsiConfigFile, volumeID.ClusterID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to get mons: %w", err,
		)
	}

	radosNS, err := config.GetRBDRadosNamespace(
		config.CsiConfigFile, volumeID.ClusterID,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to get RBD rados namespace: %w",
			err,
		)
	}

	cc, err := cephrbd.NewClusterConnection(mons)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to connect to cluster: %w", err,
		)
	}

	sc := &sourceContext{
		mons:     mons,
		radosNS:  radosNS,
		volumeID: volumeID,
	}

	return sc, cc, nil
}

// resolveParentImage determines the parent image, pool,
// namespace, and snapshot IDs based on snapshot handles.
func (w *SourceWorker) resolveParentImage(
	cc *connection.ClusterConnection, sc *sourceContext,
) error {
	baseSnapshotHandle := os.Getenv(
		"BASE_SNAPSHOT_HANDLE",
	)
	targetSnapshotHandle := os.Getenv(
		"TARGET_SNAPSHOT_HANDLE",
	)

	if baseSnapshotHandle == "" &&
		targetSnapshotHandle == "" {
		return w.resolveFullDiffFromVolume(cc, sc)
	}

	return w.resolveSnapshotDiff(
		cc, sc, baseSnapshotHandle,
		targetSnapshotHandle,
	)
}

// resolveFullDiffFromVolume sets up sourceContext for a
// full diff (no snapshots) from the volume image.
func (w *SourceWorker) resolveFullDiffFromVolume(
	cc *connection.ClusterConnection, sc *sourceContext,
) error {
	sc.parentPoolID = sc.volumeID.LocationID
	sc.parentNS = sc.radosNS
	sc.parentImageName = "csi-vol-" +
		sc.volumeID.ObjectUUID

	poolName, err := cephrbd.PoolNameByID(
		cc, sc.volumeID.LocationID,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to resolve volume pool: %w", err,
		)
	}
	sc.parentPoolName = poolName

	w.Logger.Info(
		"Using full diff (no snapshots)",
		"image", sc.parentImageName,
	)

	return nil
}

// resolveSnapshotDiff resolves parent image info from
// target and optional base snapshot handles.
//
//nolint:cyclop // snapshot resolution with incremental diff
func (w *SourceWorker) resolveSnapshotDiff(
	cc *connection.ClusterConnection, sc *sourceContext,
	baseSnapshotHandle, targetSnapshotHandle string,
) error {
	targetSnapCSI := &volid.CSIIdentifier{}
	if err := targetSnapCSI.DecomposeCSIID(
		targetSnapshotHandle,
	); err != nil {
		return fmt.Errorf(
			"failed to decompose "+
				"TARGET_SNAPSHOT_HANDLE: %w",
			err,
		)
	}

	var baseSnapCSI *volid.CSIIdentifier
	if baseSnapshotHandle != "" {
		baseSnapCSI = &volid.CSIIdentifier{}
		if err := baseSnapCSI.DecomposeCSIID(
			baseSnapshotHandle,
		); err != nil {
			return fmt.Errorf(
				"failed to decompose "+
					"BASE_SNAPSHOT_HANDLE: %w",
				err,
			)
		}
	}

	targetSnapName := "csi-snap-" +
		targetSnapCSI.ObjectUUID

	targetPoolName, err := cephrbd.PoolNameByID(
		cc, targetSnapCSI.LocationID,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to resolve target pool: %w",
			err,
		)
	}

	targetImageName := "csi-snap-" +
		targetSnapCSI.ObjectUUID
	targetSpec := cephrbd.RBDImageSpec(
		targetPoolName, sc.radosNS, targetImageName,
	)
	targetImage, err := cephrbd.NewImage(
		cc, targetSpec,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to open target image %s: %w",
			targetSpec, err,
		)
	}

	parentInfo, err := targetImage.GetParent()
	_ = targetImage.Close()
	if err != nil {
		return fmt.Errorf(
			"failed to get parent from "+
				"target snap: %w",
			err,
		)
	}
	if parentInfo == nil {
		return fmt.Errorf(
			"target snapshot has no parent",
		)
	}

	sc.parentPoolName = parentInfo.Image.PoolName
	sc.parentNS = parentInfo.Image.PoolNamespace
	sc.parentImageName = parentInfo.Image.ImageName
	sc.parentPoolID = int64(parentInfo.Image.PoolID) //nolint:gosec // G115: pool ID within safe range
	sc.targetSnapID = parentInfo.Snap.ID

	if baseSnapCSI != nil {
		baseSnapName := "csi-snap-" +
			baseSnapCSI.ObjectUUID

		baseSpec := cephrbd.RBDImageSpec(
			sc.parentPoolName, sc.parentNS,
			baseSnapName,
		)
		baseImage, err := cephrbd.NewImage(
			cc, baseSpec,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to open base "+
					"image %s: %w",
				baseSpec, err,
			)
		}

		bParentInfo, err := baseImage.GetParent()
		if err != nil {
			_ = baseImage.Close()
			return fmt.Errorf(
				"failed to get parent from "+
					"base snap: %w",
				err,
			)
		}

		if bParentInfo == nil {
			_ = baseImage.Close()
			return fmt.Errorf(
				"base snapshot has no parent",
			)
		}
		sc.fromSnapID = bParentInfo.Snap.ID
		_ = baseImage.Close()

		w.Logger.Info(
			"Using incremental diff",
			"baseSnap", baseSnapName,
			"targetSnap", targetSnapName,
			"fromSnapID", sc.fromSnapID,
			"targetSnapID", sc.targetSnapID,
		)
	} else {
		w.Logger.Info(
			"Using full diff (no base snapshot)",
			"targetSnap", targetSnapName,
		)
	}

	return nil
}

// rbdIterAdapter adapts cephrbd.RBDBlockDiffIterator to pipeline.BlockIterator.
type rbdIterAdapter struct {
	iter      *cephrbd.RBDBlockDiffIterator
	reqID     uint64
	totalSize int64
}

func (a *rbdIterAdapter) Next() (*pipeline.ChangeBlock, bool) {
	cb, ok := a.iter.Next()
	if !ok {
		return nil, false
	}
	block := &pipeline.ChangeBlock{
		FilePath:  constant.DevicePath,
		Offset:    cb.Offset,
		Len:       cb.Len,
		ReqID:     a.reqID,
		TotalSize: a.totalSize,
	}
	a.reqID++
	return block, true
}

func (a *rbdIterAdapter) Close() error {
	return a.iter.Close()
}

// fileDataReader adapts os.File to pipeline.DataReader.
type fileDataReader struct {
	file *os.File
}

func (f *fileDataReader) ReadAt(
	_ string, offset, length int64,
) ([]byte, error) {
	data := make([]byte, length)
	n, err := f.file.ReadAt(data, offset)
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("pread at offset %d: %w", offset, err)
	}
	return data[:n], nil
}

func (f *fileDataReader) CloseFile(_ string) error {
	return nil
}

// closeAndSignalDone signals done to the destination.
// Streams are closed inside individual dataSendWorkers.
func (w *SourceWorker) closeAndSignalDone(
	ctx context.Context,
	conn *grpc.ClientConn,
) error {
	w.Logger.Info("Block diff sync completed")

	return common.SignalDone(ctx, w.Logger, conn)
}
