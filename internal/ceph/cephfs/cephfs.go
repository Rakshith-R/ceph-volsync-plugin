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
	"fmt"
	"path"
	"strings"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/connection"
	cepherr "github.com/RamenDR/ceph-volsync-plugin/internal/ceph/errors"
	gocephfs "github.com/ceph/go-ceph/cephfs"
	ca "github.com/ceph/go-ceph/cephfs/admin"
)

// SnapshotDiffer provides factory methods for creating snapshot diff iterators.
// It holds the connection, mount, and snapshot names needed for comparison operations.
//
// Usage:
//
//	differ, err := cephfs.New(mons, creds, fsName, subVolumeGroup, subVolumeName, "snap1", "snap2")
//	if err != nil { return err }
//	defer differ.Destroy()
//
//	iterator, err := differ.NewSnapDiffIterator(".")
//	if err != nil { return err }
//	defer iterator.Close()
//	for {
//	    entry, err := iterator.Read()
//	    if err != nil { return err }
//	    if entry == nil { break }
//	    // Process entry
//	}
type SnapshotDiffer struct {
	conn           *connection.ClusterConnection
	mountInfo      *gocephfs.MountInfo
	rootPath       string
	relPath        string
	baseSnapName   string
	targetSnapName string
}

// New creates a new SnapshotDiffer with the given credentials and snapshot names.
// It establishes a cluster connection, mounts the CephFS at the subvolume path,
// and prepares for snapshot comparison operations.
//
// The caller must call Destroy() when done to clean up resources.
func New(
	mons string,
	fsID int64,
	subVolumeGroup string,
	subVolumeName string,
	baseSnapName string,
	targetSnapName string,
) (*SnapshotDiffer, error) {
	// Create and connect to cluster
	cc := &connection.ClusterConnection{}
	if err := cc.Connect(mons); err != nil {
		return nil, fmt.Errorf("failed to connect to cluster: %w", err)
	}

	// Get FSAdmin to resolve subvolume path
	fsa, err := cc.GetFSAdmin()
	if err != nil {
		cc.Destroy()
		return nil, fmt.Errorf("failed to get FSAdmin: %w", err)
	}

	fsName, err := GetFSName(fsa, fsID)
	if err != nil {
		cc.Destroy()
		return nil, fmt.Errorf("failed to get filesystem name for ID %d: %w", fsID, err)
	}

	// Get the subvolume path for restricted user support
	subVolumePath, err := fsa.SubVolumePath(fsName, subVolumeGroup, subVolumeName)
	if err != nil {
		cc.Destroy()
		return nil, fmt.Errorf("failed to get subvolume path: %w", err)
	}
	rootPath := path.Dir(subVolumePath)
	relPath := path.Base(subVolumePath)

	// Create mount from RADOS connection
	mountInfo, err := cc.CreateMountFromRados()
	if err != nil {
		cc.Destroy()
		return nil, fmt.Errorf("failed to create cephfs from rados: %w", err)
	}

	// Mount directly at the subvolume path to support restricted ceph users
	// who only have access to this specific subvolume
	err = mountInfo.MountWithRoot(rootPath)
	if err != nil {
		_ = mountInfo.Release()
		cc.Destroy()
		return nil, fmt.Errorf("failed to mount at subvolume path %s: %w", subVolumePath, err)
	}

	// Since we mounted with MountWithRoot(subVolumePath), the root of the mount
	// is now the subvolume itself, so rootPath is "/" and relPath is the
	// relative path within the subvolume(the uuid folder).
	return &SnapshotDiffer{
		conn:           cc,
		mountInfo:      mountInfo,
		rootPath:       "/",
		relPath:        relPath,
		baseSnapName:   baseSnapName,
		targetSnapName: targetSnapName,
	}, nil
}

// Destroy cleans up the cluster connection and unmounts the filesystem.
// Must be called when done with the SnapshotDiffer.
func (sd *SnapshotDiffer) Destroy() {
	if sd.mountInfo != nil {
		_ = sd.mountInfo.Unmount()
		_ = sd.mountInfo.Release()
	}
	if sd.conn != nil {
		sd.conn.Destroy()
	}
}

// SnapDiffIterator iterates over snapshot differences for a single directory.
// It does not recurse - caller is responsible for traversing subdirectories.
type SnapDiffIterator struct {
	differ   *SnapshotDiffer
	relPath  string
	diffInfo *gocephfs.SnapDiffInfo
}

// NewSnapDiffIterator creates a new iterator for the given directory path.
// Returns only entries in that specific directory (does not recurse).
//
// The iterator must be cleaned up with Close() when done.
func (sd *SnapshotDiffer) NewSnapDiffIterator(relPath string) (*SnapDiffIterator, error) {
	relPath = path.Join(sd.relPath, relPath)
	diffConfig := gocephfs.SnapDiffConfig{
		CMount:   sd.mountInfo,
		RootPath: sd.rootPath,
		RelPath:  relPath,
		Snap1:    sd.baseSnapName,
		Snap2:    sd.targetSnapName,
	}

	diffInfo, err := gocephfs.OpenSnapDiff(diffConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to open snap diff for %s: %w", relPath, err)
	}

	return &SnapDiffIterator{
		differ:   sd,
		relPath:  relPath,
		diffInfo: diffInfo,
	}, nil
}

// Read retrieves the next entry from the directory.
// Returns nil when there are no more entries.
func (s *SnapDiffIterator) Read() (*gocephfs.SnapDiffEntry, error) {
	entry, err := s.diffInfo.Readdir()
	if err != nil {
		return nil, fmt.Errorf("failed to read snap diff entry: %w", err)
	}

	return entry, nil
}

// SubstituteRelPath returns the full path to the entry by substituting the relative path
// with the local relative path.
func (s *SnapDiffIterator) SubstituteRelPath(entryPath, localRelPath string) string {
	// Remove the UUID folder prefix (s.differ.relPath) from the entry path
	relativePath := strings.TrimPrefix(entryPath, s.differ.relPath)
	// Remove leading slash if present after trimming
	relativePath = strings.TrimPrefix(relativePath, "/")
	// Join with the local relative path
	return path.Join(localRelPath, relativePath)
}

// Close releases the snap diff resources.
// Should be called when done reading entries.
func (s *SnapDiffIterator) Close() error {
	if s.diffInfo != nil {
		return s.diffInfo.Close()
	}
	return nil
}

// BlockDiffIterator provides iteration over changed blocks within a single file.
// It wraps the CephFS FileBlockDiffInfo API for convenient usage.
type BlockDiffIterator struct {
	differ    *SnapshotDiffer
	relPath   string
	blockDiff *gocephfs.FileBlockDiffInfo
}

// NewBlockDiffIterator creates a new iterator for block-level differences
// within the file at the given relative path.
//
// Returns an error if the block diff cannot be initialized.
// The iterator must be cleaned up with Close() when done.
func (sd *SnapshotDiffer) NewBlockDiffIterator(relPath string) (*BlockDiffIterator, error) {
	relPath = path.Join(sd.relPath, relPath)
	blockDiff, err := gocephfs.FileBlockDiffInit(
		sd.mountInfo,
		sd.rootPath,
		relPath,
		sd.baseSnapName,
		sd.targetSnapName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize block diff for %s: %w", relPath, err)
	}

	return &BlockDiffIterator{
		differ:    sd,
		relPath:   relPath,
		blockDiff: blockDiff,
	}, nil
}

// More returns true if there are more changed blocks to read.
func (b *BlockDiffIterator) More() bool {
	return b.blockDiff.More()
}

// Read retrieves the next set of changed blocks.
// Returns the changed blocks or an error if the read fails.
func (b *BlockDiffIterator) Read() (*gocephfs.FileBlockDiffChangedBlocks, error) {
	blocks, err := b.blockDiff.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read block diff for %s: %w", b.relPath, err)
	}

	return blocks, nil
}

// Close releases the block diff resources.
// Should be called when done reading blocks.
func (b *BlockDiffIterator) Close() error {
	if b.blockDiff != nil {
		return b.blockDiff.Close()
	}
	return nil
}

// GetFSName finds the filesystem name by locationID from FSAdmin.
// Returns ErrKeyNotFound if no matching filesystem is found.
func GetFSName(fsa *ca.FSAdmin, locationID int64) (string, error) {
	volumes, err := fsa.EnumerateVolumes()
	if err != nil {
		return "", err
	}

	for _, val := range volumes {
		if val.ID == locationID {
			return val.Name, nil
		}
	}

	return "", cepherr.ErrKeyNotFound
}
