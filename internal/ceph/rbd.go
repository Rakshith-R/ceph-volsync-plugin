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
