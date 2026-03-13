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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph"
	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/config"
	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/version/v1"
)

const (
	// sourceConnectionTimeout is the timeout for the first RPC call
	// which establishes the connection.
	sourceConnectionTimeout = 60 * time.Second

	// sourceRPCTimeout is the timeout for subsequent RPC calls.
	sourceRPCTimeout = 30 * time.Second

	// sourceWritePayloadMinSize is the minimum accumulated data
	// payload size before sending a WriteRequest over the gRPC stream.
	sourceWritePayloadMinSize = 2 * 1024 * 1024 // 2MB

	// sourceWritePayloadMaxSize is the maximum accumulated data
	// payload size. Prevents exceeding the 4MB gRPC default server
	// max receive message size.
	sourceWritePayloadMaxSize = 3 * 1024 * 1024 // 3MB
)

// SourceConfig holds configuration for the RBD source worker.
type SourceConfig struct {
	DestinationAddress string
}

// SourceWorker represents an RBD source worker instance.
type SourceWorker struct {
	logger logr.Logger
	config SourceConfig
}

// NewSourceWorker creates a new RBD source worker.
func NewSourceWorker(
	logger logr.Logger, config SourceConfig,
) *SourceWorker {
	return &SourceWorker{
		logger: logger.WithName("rbd-source-worker"),
		config: config,
	}
}

// Run starts the RBD source worker.
//
//nolint:cyclop,funlen // sequential orchestration steps
func (w *SourceWorker) Run(ctx context.Context) error {
	w.logger.Info("Starting RBD source worker")

	conn, err := grpc.NewClient(
		w.config.DestinationAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create gRPC client for %s: %w",
			w.config.DestinationAddress, err,
		)
	}
	defer conn.Close()

	// Verify connection with GetVersion
	versionClient := versionv1.NewVersionServiceClient(conn)
	callCtx, cancel := context.WithTimeout(
		ctx, sourceConnectionTimeout,
	)
	defer cancel()

	resp, err := versionClient.GetVersion(
		callCtx, &versionv1.GetVersionRequest{},
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get version from destination %s: %w",
			w.config.DestinationAddress, err,
		)
	}
	w.logger.Info(
		"Connected to destination",
		"version", resp.GetVersion(),
	)

	volumeHandle := os.Getenv("VOLUME_HANDLE")
	baseSnapshotHandle := os.Getenv("BASE_SNAPSHOT_HANDLE")
	targetSnapshotHandle := os.Getenv("TARGET_SNAPSHOT_HANDLE")

	// If no snapshot handles, do a full block device copy
	if baseSnapshotHandle == "" && targetSnapshotHandle == "" {
		return w.fullBlockCopy(ctx, conn)
	}

	// Parse handles
	volumeID := &ceph.CSIIdentifier{}
	if err := volumeID.DecomposeCSIID(volumeHandle); err != nil {
		return fmt.Errorf(
			"failed to decompose VOLUME_HANDLE: %w", err,
		)
	}

	targetSnapID := &ceph.CSIIdentifier{}
	if err := targetSnapID.DecomposeCSIID(
		targetSnapshotHandle,
	); err != nil {
		return fmt.Errorf(
			"failed to decompose TARGET_SNAPSHOT_HANDLE: %w",
			err,
		)
	}

	var baseSnapID *ceph.CSIIdentifier
	if baseSnapshotHandle != "" {
		baseSnapID = &ceph.CSIIdentifier{}
		if err := baseSnapID.DecomposeCSIID(
			baseSnapshotHandle,
		); err != nil {
			return fmt.Errorf(
				"failed to decompose BASE_SNAPSHOT_HANDLE: %w",
				err,
			)
		}
	}

	// Read credentials
	creds, err := readMountedRBDCredentials()
	if err != nil {
		return fmt.Errorf(
			"failed to get ceph credentials: %w", err,
		)
	}

	// Read the actual key from the key file
	userKey, err := os.ReadFile(creds.KeyFile)
	if err != nil {
		return fmt.Errorf(
			"failed to read key file %s: %w",
			creds.KeyFile, err,
		)
	}
	user := creds.ID
	key := string(userKey)

	// Get monitors
	mons, err := config.Mons(
		config.CsiConfigFile, volumeID.ClusterID,
	)
	if err != nil {
		return fmt.Errorf("failed to get mons: %w", err)
	}

	// Get RBD RADOS namespace
	radosNS, err := config.GetRBDRadosNamespace(
		config.CsiConfigFile, volumeID.ClusterID,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to get RBD rados namespace: %w", err,
		)
	}

	// Use the target snapshot to find the parent image.
	// The target snapshot's GetParent() reliably identifies
	// the source RBD image regardless of how it was cloned.
	targetSnapName := "csi-snap-" + targetSnapID.ObjectUUID

	cc, err := ceph.NewClusterConnection(mons, user, key)
	if err != nil {
		return fmt.Errorf(
			"failed to connect to cluster: %w", err,
		)
	}
	defer cc.Destroy()

	targetPoolName, err := ceph.PoolNameByID(
		cc, targetSnapID.LocationID,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to resolve target pool: %w", err,
		)
	}

	targetImageName := "csi-snap-" + targetSnapID.ObjectUUID
	targetSpec := ceph.RBDImageSpec(
		targetPoolName, radosNS, targetImageName,
	)
	targetImage, err := ceph.NewImage(cc, targetSpec)
	if err != nil {
		return fmt.Errorf(
			"failed to open target image %s: %w",
			targetSpec, err,
		)
	}

	parentInfo, err := targetImage.GetParent()
	targetImage.Close()
	if err != nil {
		return fmt.Errorf(
			"failed to get parent from target snap: %w",
			err,
		)
	}
	if parentInfo == nil {
		return fmt.Errorf("target snapshot has no parent")
	}

	// Parent image is the actual RBD volume to diff
	parentPoolName := parentInfo.Image.PoolName
	parentNS := parentInfo.Image.PoolNamespace
	parentImageName := parentInfo.Image.ImageName
	parentPoolID := int64(parentInfo.Image.PoolID)
	targetParentSnapID := parentInfo.Snap.ID

	// Determine fromSnapID for incremental diff
	var fromSnapID uint64
	if baseSnapID != nil {
		baseSnapName := "csi-snap-" + baseSnapID.ObjectUUID

		parentSpec := ceph.RBDImageSpec(
			parentPoolName, parentNS, parentImageName,
		)
		parentImage, err := ceph.NewImage(cc, parentSpec)
		if err != nil {
			return fmt.Errorf(
				"failed to open parent image %s: %w",
				parentSpec, err,
			)
		}

		fromSnapID, err = ceph.SnapshotIDByName(
			parentImage, baseSnapName,
		)
		parentImage.Close()
		if err != nil {
			return fmt.Errorf(
				"failed to find base snap ID: %w", err,
			)
		}

		w.logger.Info(
			"Using incremental diff",
			"baseSnap", baseSnapName,
			"targetSnap", targetSnapName,
			"fromSnapID", fromSnapID,
		)
	} else {
		w.logger.Info(
			"Using full diff (no base snapshot)",
			"targetSnap", targetSnapName,
		)
	}

	// Get volume size from parent image
	parentSpec := ceph.RBDImageSpec(
		parentPoolName, parentNS, parentImageName,
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
		parentImage.Close()
		return fmt.Errorf(
			"failed to get volume size: %w", err,
		)
	}
	parentImage.Close()

	// Create RBD block diff iterator
	iter, err := ceph.NewRBDBlockDiffIterator(
		mons, user, key,
		parentPoolID, radosNS, parentImageName,
		fromSnapID, targetParentSnapID, volSize,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create block diff iterator: %w", err,
		)
	}
	defer iter.Close()

	// Open block device for reading
	device, err := os.Open(devicePath)
	if err != nil {
		return fmt.Errorf(
			"failed to open %s: %w", devicePath, err,
		)
	}
	defer device.Close()

	// Create gRPC data client and sync stream
	dataClient := apiv1.NewDataServiceClient(conn)
	stream, err := dataClient.Sync(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to create sync stream: %w", err,
		)
	}

	// Iterate over changed blocks and send them
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

		isZero := isAllZero(data)
		protoBlock := &apiv1.ChangedBlock{
			Offset: uint64(block.Offset),
			Length: uint64(block.Len),
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

		// Flush at max threshold
		if accumulatedPayloadSize >= sourceWritePayloadMaxSize {
			if err := sendBlockWrite(
				stream, devicePath, accumulatedBlocks,
			); err != nil {
				return err
			}
			accumulatedBlocks = nil
			accumulatedPayloadSize = 0
		}

		// Flush at min threshold
		if accumulatedPayloadSize >= sourceWritePayloadMinSize {
			if err := sendBlockWrite(
				stream, devicePath, accumulatedBlocks,
			); err != nil {
				return err
			}
			accumulatedBlocks = nil
			accumulatedPayloadSize = 0
		}
	}

	// Flush remaining
	if len(accumulatedBlocks) > 0 {
		if err := sendBlockWrite(
			stream, devicePath, accumulatedBlocks,
		); err != nil {
			return err
		}
	}

	// Send CommitRequest
	if err := stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Commit{
			Commit: &apiv1.CommitRequest{
				Path: devicePath,
			},
		},
	}); err != nil {
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

	// Signal Done
	doneClient := apiv1.NewDoneServiceClient(conn)
	doneCtx, doneCancel := context.WithTimeout(
		ctx, sourceRPCTimeout,
	)
	defer doneCancel()

	if _, err := doneClient.Done(
		doneCtx, &apiv1.DoneRequest{},
	); err != nil {
		return fmt.Errorf(
			"failed to send Done signal: %w", err,
		)
	}

	w.logger.Info("Successfully sent Done signal")
	return nil
}

// fullBlockCopy reads the entire block device sequentially and
// sends all blocks via gRPC.
func (w *SourceWorker) fullBlockCopy(
	ctx context.Context, conn *grpc.ClientConn,
) error {
	w.logger.Info(
		"No snapshot handles, performing full block copy",
	)

	device, err := os.Open(devicePath)
	if err != nil {
		return fmt.Errorf(
			"failed to open %s: %w", devicePath, err,
		)
	}
	defer device.Close()

	fi, err := device.Stat()
	if err != nil {
		return fmt.Errorf(
			"failed to stat %s: %w", devicePath, err,
		)
	}
	totalSize := fi.Size()

	dataClient := apiv1.NewDataServiceClient(conn)
	stream, err := dataClient.Sync(ctx)
	if err != nil {
		return fmt.Errorf(
			"failed to create sync stream: %w", err,
		)
	}

	var accumulatedBlocks []*apiv1.ChangedBlock
	accumulatedPayloadSize := 0
	blockSize := int64(sourceWritePayloadMinSize)
	offset := int64(0)

	for offset < totalSize {
		readLen := blockSize
		if offset+readLen > totalSize {
			readLen = totalSize - offset
		}

		data := make([]byte, readLen)
		n, err := device.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			return fmt.Errorf(
				"failed to read at offset %d: %w",
				offset, err,
			)
		}
		data = data[:n]
		if n == 0 {
			break
		}

		isZero := isAllZero(data)
		protoBlock := &apiv1.ChangedBlock{
			Offset: uint64(offset),
			Length: uint64(n),
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

		if accumulatedPayloadSize >= sourceWritePayloadMaxSize {
			if err := sendBlockWrite(
				stream, devicePath, accumulatedBlocks,
			); err != nil {
				return err
			}
			accumulatedBlocks = nil
			accumulatedPayloadSize = 0
		}

		offset += int64(n)
	}

	if len(accumulatedBlocks) > 0 {
		if err := sendBlockWrite(
			stream, devicePath, accumulatedBlocks,
		); err != nil {
			return err
		}
	}

	// Commit
	if err := stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Commit{
			Commit: &apiv1.CommitRequest{
				Path: devicePath,
			},
		},
	}); err != nil {
		return fmt.Errorf(
			"failed to send commit: %w", err,
		)
	}

	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf(
			"failed to close sync stream: %w", err,
		)
	}

	w.logger.Info("Full block copy completed")

	// Signal Done
	doneClient := apiv1.NewDoneServiceClient(conn)
	doneCtx, doneCancel := context.WithTimeout(
		ctx, sourceRPCTimeout,
	)
	defer doneCancel()

	if _, err := doneClient.Done(
		doneCtx, &apiv1.DoneRequest{},
	); err != nil {
		return fmt.Errorf(
			"failed to send Done signal: %w", err,
		)
	}

	w.logger.Info("Successfully sent Done signal")
	return nil
}

// sendBlockWrite sends a batch of accumulated blocks as a
// single WriteRequest on the given stream.
func sendBlockWrite(
	stream grpc.ClientStreamingClient[
		apiv1.SyncRequest, apiv1.SyncResponse,
	],
	path string,
	blocks []*apiv1.ChangedBlock,
) error {
	if err := stream.Send(&apiv1.SyncRequest{
		Operation: &apiv1.SyncRequest_Write{
			Write: &apiv1.WriteRequest{
				Path:   path,
				Blocks: blocks,
			},
		},
	}); err != nil {
		return fmt.Errorf(
			"failed to send write blocks for %s: %w",
			path, err,
		)
	}
	return nil
}

// isAllZero checks if a byte slice contains only zeros.
func isAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

// readMountedRBDCredentials reads ceph admin credentials
// from a JSON file mounted at
// /etc/ceph-csi-secret/credentials.json.
func readMountedRBDCredentials() (
	*ceph.Credentials, error,
) {
	path := filepath.Join(
		CsiSecretMountPath,
		CsiSecretJSONKey,
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
