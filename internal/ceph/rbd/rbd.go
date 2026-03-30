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
	"fmt"
	"strings"
	"sync"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/connection"
	gorbd "github.com/ceph/go-ceph/rbd"
)

// ChangeBlock represents a changed block region.
type ChangeBlock struct {
	Offset int64
	Len    int64
}

// NewClusterConnection creates a new cluster connection
// using the provided monitors, user, and key.
func NewClusterConnection(
	monitors string,
) (*connection.ClusterConnection, error) {
	cc := &connection.ClusterConnection{}
	if err := cc.Connect(monitors); err != nil {
		return nil, fmt.Errorf(
			"failed to connect to cluster: %w", err,
		)
	}
	return cc, nil
}

// NewImage parses the imageSpec, connects to the pool,
// and returns an rbd.Image object.
func NewImage(
	cc *connection.ClusterConnection, imageSpec string,
) (*gorbd.Image, error) {
	parts := strings.Split(imageSpec, "/")
	if len(parts) != 2 && len(parts) != 3 {
		return nil, fmt.Errorf(
			"invalid imageSpec format, expected " +
				"'pool/imageName' or " +
				"'pool/radosNS/imageName'",
		)
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

	image, err := gorbd.OpenImage(
		ioctx, imageName, gorbd.NoSnapshot,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to open image %s in pool %s: %w",
			imageName, pool, err,
		)
	}
	return image, nil
}

// NewImageBySpec opens an RBD image from an rbd.ImageSpec.
func NewImageBySpec(
	cc *connection.ClusterConnection, spec gorbd.ImageSpec,
) (*gorbd.Image, error) {
	imageSpec := fmt.Sprintf(
		"%s/%s", spec.PoolName, spec.ImageName,
	)
	if spec.PoolNamespace != "" {
		imageSpec = fmt.Sprintf(
			"%s/%s/%s",
			spec.PoolName,
			spec.PoolNamespace,
			spec.ImageName,
		)
	}
	return NewImage(cc, imageSpec)
}

// PoolNameByID resolves a Ceph pool ID to its name.
func PoolNameByID(
	cc *connection.ClusterConnection, poolID int64,
) (string, error) {
	name, err := cc.GetPoolByID(poolID)
	if err != nil {
		return "", fmt.Errorf(
			"failed to resolve pool ID %d: %w",
			poolID, err,
		)
	}
	return name, nil
}

// RBDImageSpec builds a "pool/[ns/]image" string.
func RBDImageSpec(
	pool, radosNS, imageName string,
) string {
	if radosNS != "" {
		return fmt.Sprintf(
			"%s/%s/%s", pool, radosNS, imageName,
		)
	}
	return fmt.Sprintf("%s/%s", pool, imageName)
}

// SnapshotIDByName finds the snapshot ID for a named
// snapshot on an RBD image.
func SnapshotIDByName(
	image *gorbd.Image, snapName string,
) (uint64, error) {
	snaps, err := image.GetSnapshotNames()
	if err != nil {
		return 0, fmt.Errorf(
			"failed to get snapshot names: %w", err,
		)
	}
	for _, s := range snaps {
		if s.Name == snapName {
			return s.Id, nil
		}
	}
	return 0, fmt.Errorf(
		"snapshot %q not found", snapName,
	)
}

// RBDBlockDiffIterator iterates over changed blocks
// between two snapshots of an RBD image.
type RBDBlockDiffIterator struct {
	conn       *connection.ClusterConnection
	image      *gorbd.Image
	blocksChan chan ChangeBlock
	doneChan   chan error
	closeOnce  sync.Once
	closeErr   error
}

// NewRBDBlockDiffIterator creates a new iterator that
// reports changed blocks between fromSnapID and
// targetSnapID on the image.
func NewRBDBlockDiffIterator(
	monitors string,
	poolID int64,
	radosNamespace string,
	imageName string,
	fromSnapID uint64,
	targetSnapID uint64,
	volSize uint64,
) (*RBDBlockDiffIterator, error) {
	conn, err := NewClusterConnection(monitors)
	if err != nil {
		return nil, err
	}

	poolName, err := PoolNameByID(conn, poolID)
	if err != nil {
		conn.Destroy()
		return nil, fmt.Errorf(
			"failed to resolve pool: %w", err,
		)
	}

	imageSpec := RBDImageSpec(
		poolName, radosNamespace, imageName,
	)
	image, err := NewImage(conn, imageSpec)
	if err != nil {
		conn.Destroy()
		return nil, fmt.Errorf(
			"failed to open image %s: %w",
			imageSpec, err,
		)
	}
	if targetSnapID != 0 {
		if err := image.SetSnapByID(targetSnapID); err != nil {
			_ = image.Close()
			conn.Destroy()
			return nil, fmt.Errorf(
				"failed to set snap context to %d: %w",
				targetSnapID, err,
			)
		}
	}

	blocksChan := make(chan ChangeBlock, 64)
	doneChan := make(chan error, 1)

	diffCfg := gorbd.DiffIterateByIDConfig{
		Offset:        0,
		Length:        volSize,
		IncludeParent: gorbd.IncludeParent,
		WholeObject:   gorbd.EnableWholeObject,
		Callback: func(
			offset uint64,
			length uint64,
			_ int,
			_ interface{},
		) int {
			blocksChan <- ChangeBlock{
				Offset: int64(offset), //nolint:gosec // G115: RBD offsets are within safe range
				Len:    int64(length), //nolint:gosec // G115: RBD lengths are within safe range
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

// Next returns the next changed block.
func (it *RBDBlockDiffIterator) Next() (
	*ChangeBlock, bool,
) {
	block, ok := <-it.blocksChan
	if !ok {
		return nil, false
	}
	return &block, true
}

// Close drains remaining blocks, waits for the diff
// goroutine to finish, then releases the image and
// cluster connection. Must be called even on error
// to avoid goroutine/image leaks.
func (it *RBDBlockDiffIterator) Close() error {
	it.closeOnce.Do(func() {
		// Drain blocksChan so the diff goroutine can
		// finish if it's blocked on a channel send.
		for range it.blocksChan {
		}

		// Wait for the diff goroutine to complete.
		iterErr := <-it.doneChan

		var closeErr error
		if it.image != nil {
			closeErr = it.image.Close()
		}
		if it.conn != nil {
			it.conn.Destroy()
		}

		if iterErr != nil {
			it.closeErr = fmt.Errorf(
				"diff iteration error: %w", iterErr,
			)
		} else {
			it.closeErr = closeErr
		}
	})
	return it.closeErr
}
