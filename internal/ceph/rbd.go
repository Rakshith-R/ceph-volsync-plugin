package ceph

import (
	"fmt"
	"os"
	"strings"

	wp "github.com/RamenDR/ceph-volsync-plugin/internal/workerpool"
	"github.com/ceph/go-ceph/rbd"
)

type DiffConfig struct {
	Monitors     string
	User         string
	UserKey      string
	TargetImage  string
	BaseImage    string
	TargetFile   *os.File
	BaseFile     *os.File
	NumDivisions int // Number of divisions for diff processing
}

type ChangeBlock struct {
	Offset int64
	Len    int64
}
type ChunkPair struct {
	Offset int64
	Source []byte
	Target []byte
}

func (dc *DiffConfig) Process(readerWorkers int, chunkPairQueue *chan ChunkPair) error {
	var baseImage *rbd.Image
	conn, err := NewClusterConnection(dc.Monitors, dc.User, dc.UserKey)
	if err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	image, err := NewImage(conn, dc.TargetImage)
	if err != nil {
		return fmt.Errorf("failed to open target image: %w", err)
	}

	parentInfo, err := image.GetParent()
	if err != nil {
		return fmt.Errorf("failed to get parent info: %w", err)
	}
	if parentInfo == nil {
		return fmt.Errorf("target image has no parent")
	}
	err = image.Close()
	if err != nil {
		return fmt.Errorf("failed to close target image: %w", err)
	}

	parentImage, err := NewImageBySpec(conn, parentInfo.Image)
	if err != nil {
		return fmt.Errorf("failed to open parent image: %w", err)
	}

	err = parentImage.SetSnapByID(parentInfo.Snap.ID)
	if err != nil {
		return fmt.Errorf("failed to set snapshot by ID: %w", err)
	}
	changedBlockQueue := make(chan ChangeBlock, readerWorkers)
	volSize, err := parentImage.GetSize()
	if err != nil {
		return fmt.Errorf("failed to get image size: %w", err)
	}
	DiffConfig := rbd.DiffIterateByIDConfig{
		Offset:        0,
		Length:        volSize,
		IncludeParent: rbd.IncludeParent,
		WholeObject:   rbd.EnableWholeObject,
		Callback: func(offset uint64, length uint64, _ int, _ interface{}) int {

			for i := 0; i < dc.NumDivisions; i++ {
				changedBlockQueue <- ChangeBlock{
					Offset: int64(offset + uint64(i)*length/uint64(dc.NumDivisions)),
					Len:    int64(length / uint64(dc.NumDivisions)),
				}
			}
			return 0
		},
	}
	if dc.BaseImage != "" {
		baseImage, err = NewImage(conn, dc.BaseImage)
		if err != nil {
			return fmt.Errorf("failed to open base image: %w", err)
		}
		defer baseImage.Close()
		baseParentInfo, err := baseImage.GetParent()
		if err != nil {
			return fmt.Errorf("failed to get base image parent info: %w", err)
		}
		if baseParentInfo == nil {
			return fmt.Errorf("base image has no parent")
		}
		DiffConfig.FromSnapID = baseParentInfo.Snap.ID
	}

	ReadersPool := wp.NewWorkerPool(readerWorkers)
	for i := 0; i < readerWorkers; i++ {
		fmt.Printf("Starting reader worker %d\n", i)
		ReadersPool.Submit(func() {
			for chunk := range changedBlockQueue {
				sourceBuf := make([]byte, chunk.Len)
				_, err := dc.TargetFile.ReadAt(sourceBuf, chunk.Offset)
				if err != nil {
					fmt.Printf("Failed to read source block at offset %d: %v", chunk.Offset, err)
					changedBlockQueue <- chunk
					continue
				}
				chunkPair := ChunkPair{
					Offset: chunk.Offset,
					Source: sourceBuf,
					Target: nil,
				}

				if baseImage != nil {
					targetBuf := make([]byte, chunk.Len)
					_, err = dc.BaseFile.ReadAt(targetBuf, chunk.Offset)
					if err != nil {
						fmt.Printf("Failed to read base image block at offset %d: %v", chunk.Offset, err)
						changedBlockQueue <- chunk
						continue
					}
					chunkPair.Target = targetBuf
				}

				// fmt.Printf("Processing chunk at offset %d, length %d\n", chunk.Offset, chunk.Len)
				*chunkPairQueue <- chunkPair
			}
			fmt.Printf("Reader worker finished processing\n")
		})
		fmt.Printf("Reader worker %d submitted\n", i)
	}
	go func() {
		fmt.Printf("Waiting for all reader workers to finish...\n")
		err = parentImage.DiffIterateByID(DiffConfig)
		if err != nil {
			fmt.Printf("failed to iterate over diff: %w", err)
		}
		err = parentImage.Close()
		if err != nil {
			fmt.Printf("failed to close parent image: %w", err)
		}
		fmt.Printf("Reader workers shutdown complete\n")
		close(changedBlockQueue)
		fmt.Printf("All reader workers finished processing\n")
		ReadersPool.Shutdown()
		fmt.Printf("Changed block queue closed\n")
		close(*chunkPairQueue)
		fmt.Printf("Chunk pair queue closed\n")
	}()

	return nil
}

func NewClusterConnection(monitors, user, userKey string) (*ClusterConnection, error) {
	secrets := map[string]string{
		"userID":  user,
		"userKey": userKey,
	}
	cr, err := NewUserCredentials(secrets)
	if err != nil {
		return nil, fmt.Errorf("failed to create credentials: %w", err)
	}

	cc := &ClusterConnection{}
	if err := cc.Connect(monitors, cr); err != nil {
		return nil, fmt.Errorf("failed to connect to cluster: %w", err)
	}
	return cc, nil
}

// NewImage parses the imageSpec, connects to the pool, and returns an rbd.Image object.
func NewImage(cc *ClusterConnection, imageSpec string) (*rbd.Image, error) {
	parts := strings.Split(imageSpec, "/")
	if len(parts) != 2 && len(parts) != 3 {
		return nil, fmt.Errorf("invalid imageSpec format, expected 'pool/imageName' or 'pool/radosNS/imageName'")
	}

	pool := parts[0]
	imageName := parts[len(parts)-1]
	namespace := ""
	if len(parts) == 3 {
		namespace = parts[1]
	}

	ioctx, err := cc.GetIoctx(pool)
	if err != nil {
		return nil, err
	}
	defer ioctx.Destroy()

	if namespace != "" {
		ioctx.SetNamespace(namespace)
	}

	image, err := rbd.OpenImage(ioctx, imageName, rbd.NoSnapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to open image %s in pool %s: %w", imageName, pool, err)
	}
	return image, nil
}

func NewImageBySpec(cc *ClusterConnection, spec rbd.ImageSpec) (*rbd.Image, error) {
	imageSpec := fmt.Sprintf("%s/%s", spec.PoolName, spec.ImageName)
	if spec.PoolNamespace != "" {
		imageSpec = fmt.Sprintf("%s/%s/%s", spec.PoolName, spec.PoolNamespace, spec.ImageName)
	}
	return NewImage(cc, imageSpec)
}

// PoolNameByID resolves a Ceph pool ID to its name.
func PoolNameByID(cc *ClusterConnection, poolID int64) (string, error) {
	if cc.conn == nil {
		return "", fmt.Errorf("cluster is not connected")
	}
	name, err := cc.conn.GetPoolByID(poolID)
	if err != nil {
		return "", fmt.Errorf("failed to resolve pool ID %d: %w", poolID, err)
	}
	return name, nil
}

// RBDImageSpec builds a "pool/[ns/]image" string.
func RBDImageSpec(pool, radosNS, imageName string) string {
	if radosNS != "" {
		return fmt.Sprintf("%s/%s/%s", pool, radosNS, imageName)
	}
	return fmt.Sprintf("%s/%s", pool, imageName)
}

// SnapshotIDByName finds the snapshot ID for a named snapshot on an RBD image.
func SnapshotIDByName(image *rbd.Image, snapName string) (uint64, error) {
	snaps, err := image.GetSnapshotNames()
	if err != nil {
		return 0, fmt.Errorf("failed to get snapshot names: %w", err)
	}
	for _, s := range snaps {
		if s.Name == snapName {
			return s.Id, nil
		}
	}
	return 0, fmt.Errorf("snapshot %q not found", snapName)
}

// RBDBlockDiffIterator iterates over changed blocks between
// two snapshots of an RBD image using DiffIterateByID.
type RBDBlockDiffIterator struct {
	conn       *ClusterConnection
	image      *rbd.Image
	blocksChan chan ChangeBlock
	doneChan   chan error
}

// NewRBDBlockDiffIterator creates a new iterator that reports changed blocks
// between fromSnapID and targetSnapID on the image. If fromSnapID is 0,
// a full diff (all allocated extents up to targetSnapID) is produced.
// targetSnapID sets the snap context for the diff operation.
func NewRBDBlockDiffIterator(
	monitors, user, userKey string,
	poolID int64,
	radosNamespace string,
	imageName string,
	fromSnapID uint64,
	targetSnapID uint64,
	volSize uint64,
) (*RBDBlockDiffIterator, error) {
	conn, err := NewClusterConnection(monitors, user, userKey)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to cluster: %w", err)
	}

	poolName, err := PoolNameByID(conn, poolID)
	if err != nil {
		conn.Destroy()
		return nil, fmt.Errorf("failed to resolve pool: %w", err)
	}

	imageSpec := RBDImageSpec(poolName, radosNamespace, imageName)
	image, err := NewImage(conn, imageSpec)
	if err != nil {
		conn.Destroy()
		return nil, fmt.Errorf("failed to open image %s: %w", imageSpec, err)
	}
	if err := image.SetSnapByID(targetSnapID); err != nil {
		image.Close()
		conn.Destroy()
		return nil, fmt.Errorf(
			"failed to set snap context to %d: %w",
			targetSnapID, err,
		)
	}

	blocksChan := make(chan ChangeBlock, 64)
	doneChan := make(chan error, 1)

	diffCfg := rbd.DiffIterateByIDConfig{
		Offset:        0,
		Length:        volSize,
		IncludeParent: rbd.IncludeParent,
		WholeObject:   rbd.EnableWholeObject,
		Callback: func(
			offset uint64, length uint64, _ int, _ interface{},
		) int {
			blocksChan <- ChangeBlock{
				Offset: int64(offset),
				Len:    int64(length),
			}
			return 0
		},
	}
	if fromSnapID > 0 {
		diffCfg.FromSnapID = fromSnapID
	}

	go func() {
		defer close(blocksChan)
		doneChan <- image.DiffIterateByID(diffCfg)
	}()

	return &RBDBlockDiffIterator{
		conn:       conn,
		image:      image,
		blocksChan: blocksChan,
		doneChan:   doneChan,
	}, nil
}

// Next returns the next changed block. If there are no more blocks,
// it returns (nil, false). The caller should check Close() for any
// iteration error.
func (it *RBDBlockDiffIterator) Next() (*ChangeBlock, bool) {
	block, ok := <-it.blocksChan
	if !ok {
		return nil, false
	}
	return &block, true
}

// Close releases the image and cluster connection. It also returns
// any error that occurred during diff iteration.
func (it *RBDBlockDiffIterator) Close() error {
	var iterErr error
	select {
	case iterErr = <-it.doneChan:
	default:
	}

	var closeErr error
	if it.image != nil {
		closeErr = it.image.Close()
	}
	if it.conn != nil {
		it.conn.Destroy()
	}

	if iterErr != nil {
		return fmt.Errorf("diff iteration error: %w", iterErr)
	}
	return closeErr
}
