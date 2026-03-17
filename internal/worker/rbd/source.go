//go:build ceph_preview

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

package rbd

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/config"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/volid"
	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

// SourceConfig holds configuration for the RBD source
// worker.
type SourceConfig struct {
	DestinationAddress string
}

// SourceWorker represents an RBD source worker
// instance.
type SourceWorker struct {
	logger logr.Logger
	config SourceConfig
}

// NewSourceWorker creates a new RBD source worker.
func NewSourceWorker(
	logger logr.Logger, cfg SourceConfig,
) *SourceWorker {
	return &SourceWorker{
		logger: logger.WithName("rbd-source-worker"),
		config: cfg,
	}
}

// sourceContext holds resolved state needed for the
// sync operation.
type sourceContext struct {
	mons            string
	user            string
	key             string
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
//
//nolint:funlen // sequential orchestration steps
func (w *SourceWorker) Run(
	ctx context.Context,
) (err error) {
	w.logger.Info("Starting RBD source worker")

	conn, err := common.ConnectToDestination(
		ctx, w.logger,
		w.config.DestinationAddress,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(
				common.MaxGRPCMessageSize,
			),
		),
	)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := conn.Close(); cerr != nil && err == nil {
			err = fmt.Errorf(
				"failed to close gRPC connection: %w",
				cerr,
			)
		}
	}()

	sc, cc, err := w.resolveSourceConfig()
	if err != nil {
		return err
	}
	defer cc.Destroy()

	if err := w.resolveParentImage(cc, sc); err != nil {
		return err
	}

	parentSpec := ceph.RBDImageSpec(
		sc.parentPoolName, sc.parentNS,
		sc.parentImageName,
	)
	parentImage, err := ceph.NewImage(cc, parentSpec)
	if err != nil {
		return fmt.Errorf(
			"failed to open parent image %s: %w",
			parentSpec, err,
		)
	}

	volSize, err := parentImage.GetSize()
	if err != nil {
		_ = parentImage.Close()
		return fmt.Errorf(
			"failed to get volume size: %w", err,
		)
	}
	_ = parentImage.Close()

	iter, err := ceph.NewRBDBlockDiffIterator(
		sc.mons, sc.user, sc.key,
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

	device, err := os.Open(common.DevicePath)
	if err != nil {
		return fmt.Errorf(
			"failed to open %s: %w",
			common.DevicePath, err,
		)
	}
	defer func() { _ = device.Close() }()

	dataClient := apiv1.NewDataServiceClient(conn)
	stream, err := dataClient.Sync(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to create sync stream: %w", err,
		)
	}

	if err := w.streamBlocks(
		iter, device, stream,
	); err != nil {
		return err
	}

	return w.commitAndSignalDone(ctx, conn, stream)
}

// resolveSourceConfig reads environment variables,
// credentials, and Ceph cluster configuration to
// populate a sourceContext. Returns the sourceContext
// and an active ClusterConnection that the caller must
// destroy.
func (w *SourceWorker) resolveSourceConfig() (
	*sourceContext, *ceph.ClusterConnection, error,
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

	creds, err := common.ReadMountedCredentials()
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to get ceph credentials: %w", err,
		)
	}

	userKey, err := os.ReadFile(creds.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to read key file %s: %w",
			creds.KeyFile, err,
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

	user := creds.ID
	key := string(userKey)

	cc, err := ceph.NewClusterConnection(
		mons, user, key,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to connect to cluster: %w", err,
		)
	}

	sc := &sourceContext{
		mons:     mons,
		user:     user,
		key:      key,
		radosNS:  radosNS,
		volumeID: volumeID,
	}

	return sc, cc, nil
}

// resolveParentImage determines the parent image, pool,
// namespace, and snapshot IDs based on snapshot handles.
func (w *SourceWorker) resolveParentImage(
	cc *ceph.ClusterConnection, sc *sourceContext,
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
	cc *ceph.ClusterConnection, sc *sourceContext,
) error {
	sc.parentPoolID = sc.volumeID.LocationID
	sc.parentNS = sc.radosNS
	sc.parentImageName = "csi-vol-" +
		sc.volumeID.ObjectUUID

	poolName, err := ceph.PoolNameByID(
		cc, sc.volumeID.LocationID,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to resolve volume pool: %w", err,
		)
	}
	sc.parentPoolName = poolName

	w.logger.Info(
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
	cc *ceph.ClusterConnection, sc *sourceContext,
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

	targetPoolName, err := ceph.PoolNameByID(
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
	targetSpec := ceph.RBDImageSpec(
		targetPoolName, sc.radosNS, targetImageName,
	)
	targetImage, err := ceph.NewImage(
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

		baseSpec := ceph.RBDImageSpec(
			sc.parentPoolName, sc.parentNS,
			baseSnapName,
		)
		baseImage, err := ceph.NewImage(
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

		w.logger.Info(
			"Using incremental diff",
			"baseSnap", baseSnapName,
			"targetSnap", targetSnapName,
			"fromSnapID", sc.fromSnapID,
		)
	} else {
		w.logger.Info(
			"Using full diff (no base snapshot)",
			"targetSnap", targetSnapName,
		)
	}

	return nil
}

// streamBlocks iterates over changed blocks from the
// diff iterator, reads data from the device, and sends
// batched write requests over the gRPC stream.
func (w *SourceWorker) streamBlocks(
	iter *ceph.RBDBlockDiffIterator,
	device *os.File,
	stream grpc.ClientStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
) error {
	var accumulatedBlocks []*apiv1.ChangedBlock
	accumulatedPayloadSize := 0

	for {
		block, ok := iter.Next()
		if !ok {
			break
		}

		data := make([]byte, block.Len)
		n, err := device.ReadAt(data, block.Offset)
		if err != nil && err != io.EOF {
			return fmt.Errorf(
				"failed to read at offset %d: %w",
				block.Offset, err,
			)
		}
		data = data[:n]

		isZero := common.IsAllZero(data)
		protoBlock := &apiv1.ChangedBlock{
			Offset: uint64(block.Offset), //nolint:gosec // G115: RBD offsets are non-negative
			Length: uint64(block.Len),    //nolint:gosec // G115: RBD offsets are non-negative
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
				stream, common.DevicePath,
				accumulatedBlocks,
			); err != nil {
				return err
			}
			accumulatedBlocks = nil
			accumulatedPayloadSize = 0
		}

		if accumulatedPayloadSize >=
			common.WritePayloadMinSize {
			if err := common.SendBlockWrite(
				stream, common.DevicePath,
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
			stream, common.DevicePath,
			accumulatedBlocks,
		); err != nil {
			return err
		}
	}

	return nil
}

// commitAndSignalDone sends the commit request on the
// stream and signals done to the destination.
func (w *SourceWorker) commitAndSignalDone(
	ctx context.Context,
	conn *grpc.ClientConn,
	stream grpc.ClientStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
) error {
	if err := stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Commit{
			Commit: &apiv1.CommitRequest{
				Path: common.DevicePath,
			},
		},
	}); err != nil {
		if err == io.EOF {
			if _, recvErr := stream.CloseAndRecv(); recvErr != nil {
				return fmt.Errorf(
					"destination error during "+
						"commit: %w",
					recvErr,
				)
			}
		}
		return fmt.Errorf(
			"failed to send commit: %w", err,
		)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf(
			"failed to close sync stream: %w", err,
		)
	}

	w.logger.Info("Block diff sync completed")

	return common.SignalDone(ctx, w.logger, conn)
}
