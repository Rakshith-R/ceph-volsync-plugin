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

package tunnel

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/go-logr/logr"
)

const (
	rsyncdConf = "/tmp/rsyncd.conf"
	rsyncBin   = "rsync"
	dataPath   = "/data"
)

// RsyncDaemon manages the rsync daemon subprocess.
type RsyncDaemon struct {
	port   string
	cmd    *exec.Cmd
	logger logr.Logger
}

// NewRsyncDaemon creates a new RsyncDaemon manager.
func NewRsyncDaemon(
	logger logr.Logger, port string,
) *RsyncDaemon {
	return &RsyncDaemon{
		port:   port,
		logger: logger.WithName("rsync-daemon"),
	}
}

// Start generates rsyncd.conf and starts the rsync
// daemon as a background subprocess.
func (r *RsyncDaemon) Start() error {
	if err := r.generateConfig(); err != nil {
		return fmt.Errorf(
			"failed to generate rsyncd config: %w", err,
		)
	}

	r.logger.Info(
		"Starting rsync daemon", "port", r.port,
	)

	r.cmd = exec.Command( //nolint:gosec // G204: command args are not user-controlled
		rsyncBin,
		"--daemon",
		"--config="+rsyncdConf,
		"--port="+r.port,
		"--address=127.0.0.1",
		"--no-detach",
	)
	r.cmd.Stdout = os.Stdout
	r.cmd.Stderr = os.Stderr

	if err := r.cmd.Start(); err != nil {
		return fmt.Errorf(
			"failed to start rsync daemon: %w", err,
		)
	}

	// Wait for rsync daemon to initialize
	time.Sleep(2 * time.Second)

	// Check if it's still running
	if err := r.cmd.Process.Signal(syscall.Signal(0)); err != nil {
		return fmt.Errorf("rsync daemon exited prematurely: %w", err)
	}

	r.logger.Info(
		"rsync daemon started successfully",
		"pid", r.cmd.Process.Pid,
	)

	return nil
}

// Stop sends SIGTERM to the rsync daemon and waits.
func (r *RsyncDaemon) Stop() {
	if r.cmd == nil || r.cmd.Process == nil {
		return
	}

	r.logger.Info(
		"Stopping rsync daemon",
		"pid", r.cmd.Process.Pid,
	)

	if err := r.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		r.logger.Error(
			err, "Failed to send SIGTERM to rsync daemon",
		)
	}

	_ = r.cmd.Wait()
}

func (r *RsyncDaemon) generateConfig() error {
	conf := `# Rsync daemon configuration
uid = root
gid = root
use chroot = no
max connections = 4
pid file = /tmp/rsyncd.pid
log file = /tmp/rsyncd.log
lock file = /tmp/rsyncd.lock

[data]
    path = ` + dataPath + `
    comment = Data volume
    read only = false
    list = yes
    # No authentication - stunnel already provides PSK authentication
`
	return os.WriteFile(rsyncdConf, []byte(conf), 0600)
}
