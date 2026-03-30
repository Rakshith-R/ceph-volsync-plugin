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
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph/cephconf"
	"github.com/ceph/go-ceph/rados"
)

type connEntry struct {
	conn     *rados.Conn
	lastUsed time.Time
	users    int
}

// ConnPool contains connection entries and gc params.
type ConnPool struct {
	interval time.Duration
	expiry   time.Duration
	timer    *time.Timer
	lock     *sync.RWMutex
	conns    map[string]*connEntry
}

// NewConnPool creates a new connection pool instance.
func NewConnPool(
	interval, expiry time.Duration,
) *ConnPool {
	cp := ConnPool{
		interval: interval,
		expiry:   expiry,
		lock:     &sync.RWMutex{},
		conns:    make(map[string]*connEntry),
	}
	cp.timer = time.AfterFunc(interval, cp.gc)

	return &cp
}

// Destroy stops the gc and destroys all connections.
func (cp *ConnPool) Destroy() {
	cp.timer.Stop()
	cp.lock.Lock()
	defer cp.lock.Unlock()

	for key, ce := range cp.conns {
		if ce.users != 0 {
			panic(
				"this connEntry still has users, " +
					"operations might still be in-flight",
			)
		}

		ce.destroy()
		delete(cp.conns, key)
	}
}

// Get returns a rados.Conn for the given arguments.
func (cp *ConnPool) Get(
	monitors, user, keyfile string,
) (*rados.Conn, error) {
	err := cephconf.WriteCephConfig()
	if err != nil {
		return nil, fmt.Errorf(
			"failed to write ceph config: %w", err,
		)
	}

	unique, err := cp.generateUniqueKey(
		monitors, user, keyfile,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to generate unique for connection: %w",
			err,
		)
	}

	cp.lock.Lock()
	conn := cp.getConn(unique)
	cp.lock.Unlock()
	if conn != nil {
		return conn, nil
	}

	args := []string{
		"-m", monitors, "--keyfile=" + keyfile,
	}
	conn, err = rados.NewConnWithUser(user)
	if err != nil {
		return nil, fmt.Errorf(
			"creating a new connection failed: %w", err,
		)
	}
	err = conn.ParseCmdLineArgs(args)
	if err != nil {
		return nil, fmt.Errorf(
			"parsing cmdline args (%v) failed: %w",
			args, err,
		)
	}

	if err = conn.ReadConfigFile(
		cephconf.CephConfigPath,
	); err != nil {
		return nil, fmt.Errorf(
			"failed to read config file %q: %w",
			cephconf.CephConfigPath, err,
		)
	}

	err = conn.Connect()
	if err != nil {
		return nil, fmt.Errorf(
			"connecting failed: %w", err,
		)
	}

	ce := &connEntry{
		conn:     conn,
		lastUsed: time.Now(),
		users:    1,
	}

	cp.lock.Lock()
	defer cp.lock.Unlock()
	if oldConn := cp.getConn(unique); oldConn != nil {
		ce.destroy()
		return oldConn, nil
	}
	cp.conns[unique] = ce

	return conn, nil
}

// Copy adds an extra reference count to the ConnEntry.
func (cp *ConnPool) Copy(conn *rados.Conn) *rados.Conn {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	for _, ce := range cp.conns {
		if ce.conn == conn {
			ce.get()
			return ce.conn
		}
	}

	return nil
}

// Put reduces the reference count of the rados.Conn.
func (cp *ConnPool) Put(conn *rados.Conn) {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	for _, ce := range cp.conns {
		if ce.conn == conn {
			ce.put()
			return
		}
	}
}

func (cp *ConnPool) gc() {
	cp.lock.Lock()
	defer cp.lock.Unlock()

	now := time.Now()
	for key, ce := range cp.conns {
		if ce.users == 0 &&
			(now.Sub(ce.lastUsed)) > cp.expiry {
			ce.destroy()
			delete(cp.conns, key)
		}
	}

	cp.timer.Reset(cp.interval)
}

func (cp *ConnPool) generateUniqueKey(
	monitors, user, keyfile string,
) (string, error) {
	// #nosec:G304, file inclusion via variable.
	key, err := os.ReadFile(keyfile) //nolint:gosec // file inclusion via variable
	if err != nil {
		return "", fmt.Errorf(
			"could not open keyfile %s: %w",
			keyfile, err,
		)
	}

	return fmt.Sprintf(
		"%s|%s|%s", monitors, user, string(key),
	), nil
}

func (cp *ConnPool) getConn(unique string) *rados.Conn {
	ce, exists := cp.conns[unique]
	if exists {
		ce.get()
		return ce.conn
	}

	return nil
}

func (ce *connEntry) get() {
	ce.lastUsed = time.Now()
	ce.users++
}

func (ce *connEntry) put() {
	ce.users--
}

func (ce *connEntry) destroy() {
	if ce.conn != nil {
		ce.conn.Shutdown()
		ce.conn = nil
	}
}
