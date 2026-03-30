/*
Copyright 2020 The Ceph-CSI Authors.

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

package connection

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	cepherr "github.com/RamenDR/ceph-volsync-plugin/internal/ceph/errors"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/constant"
	"github.com/ceph/go-ceph/cephfs"
	ca "github.com/ceph/go-ceph/cephfs/admin"
	"github.com/ceph/go-ceph/rados"
)

// ClusterConnection represents a connection to a Ceph cluster.
type ClusterConnection struct {
	conn *rados.Conn
}

var (
	cpInterval = 15 * time.Minute
	cpExpiry   = 10 * time.Minute
	connPool   = NewConnPool(cpInterval, cpExpiry)
)

// Connect connects to the Ceph cluster using credentials
// mounted at the ceph-csi-secret volume.
func (cc *ClusterConnection) Connect(monitors string) error {
	if cc.conn != nil {
		return nil
	}

	idPath := filepath.Join(
		constant.CsiSecretMountPath,
		constant.CsiSecretUserIDKey,
	)
	idBytes, err := os.ReadFile(idPath) //nolint:gosec // G304: path is internally constructed
	if err != nil {
		return fmt.Errorf(
			"failed to read userID from %s: %w",
			idPath, err,
		)
	}
	userID := strings.TrimSpace(string(idBytes))

	keyFilePath := filepath.Join(
		constant.CsiSecretMountPath,
		constant.CsiSecretUserKeyKey,
	)

	conn, err := connPool.Get(monitors, userID, keyFilePath)
	if err != nil {
		return fmt.Errorf(
			"failed to get connection: %w", err,
		)
	}

	cc.conn = conn
	return nil
}

// Destroy releases the cluster connection.
func (cc *ClusterConnection) Destroy() {
	if cc.conn != nil {
		connPool.Put(cc.conn)
	}
}

// GetIoctx returns a rados IOContext for the given pool.
func (cc *ClusterConnection) GetIoctx(
	pool string,
) (*rados.IOContext, error) {
	if cc.conn == nil {
		return nil, errors.New(
			"cluster is not connected yet",
		)
	}

	ioctx, err := cc.conn.OpenIOContext(pool)
	if err != nil {
		if errors.Is(err, rados.ErrNotFound) {
			err = fmt.Errorf(
				"failed as %w (internal %w)",
				cepherr.ErrPoolNotFound, err,
			)
		} else {
			err = fmt.Errorf(
				"failed to open IOContext for pool %s: %w",
				pool, err,
			)
		}

		return nil, err
	}

	return ioctx, nil
}

// GetPoolByID resolves a pool ID to its name.
func (cc *ClusterConnection) GetPoolByID(
	poolID int64,
) (string, error) {
	if cc.conn == nil {
		return "", errors.New(
			"cluster is not connected yet",
		)
	}

	return cc.conn.GetPoolByID(poolID)
}

// GetFSAdmin returns an FSAdmin for CephFS administration.
func (cc *ClusterConnection) GetFSAdmin() (
	*ca.FSAdmin, error,
) {
	if cc.conn == nil {
		return nil, errors.New(
			"cluster is not connected yet",
		)
	}

	return ca.NewFromConn(cc.conn), nil
}

// CreateMountFromRados creates a CephFS mount from the
// existing RADOS connection.
func (cc *ClusterConnection) CreateMountFromRados() (
	*cephfs.MountInfo, error,
) {
	return cephfs.CreateFromRados(cc.conn)
}
