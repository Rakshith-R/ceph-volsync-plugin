//go:build ceph_preview

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

package ceph

import (
	"errors"
	"fmt"
	"time"

	"github.com/ceph/go-ceph/rados"
)

// ClusterConnection represents a connection to a Ceph cluster.
type ClusterConnection struct {
	conn  *rados.Conn
	Creds *Credentials
}

var (
	cpInterval = 15 * time.Minute
	cpExpiry   = 10 * time.Minute
	connPool   = NewConnPool(cpInterval, cpExpiry)
)

// Connect connects to the Ceph cluster.
func (cc *ClusterConnection) Connect(
	monitors string, cr *Credentials,
) error {
	if cc.conn == nil {
		conn, err := connPool.Get(
			monitors, cr.ID, cr.KeyFile,
		)
		if err != nil {
			return fmt.Errorf(
				"failed to get connection: %w", err,
			)
		}

		cc.conn = conn
		cc.Creds = cr
	}

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
				ErrPoolNotFound, err,
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
