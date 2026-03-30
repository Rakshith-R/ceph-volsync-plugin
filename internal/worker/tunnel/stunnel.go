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
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"

	wcommon "github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
)

const (
	pskFilePath = "/keys/psk.txt"
	stunnelDir  = "/tmp/stunnel"
	stunnelConf = "/tmp/stunnel/stunnel.conf"
	stunnelPID  = "/tmp/stunnel/stunnel.pid"
	stunnelLog  = "/tmp/stunnel.log"
	stunnelBin  = "stunnel"

	// sourceGRPCSocketPath is the Unix domain socket path
	// for the source gRPC tunnel client endpoint.
	sourceGRPCSocketPath = "/tmp/stunnel/grpc.sock"

	// sourceRsyncSocketPath is the Unix domain socket path
	// for the source rsync tunnel client endpoint.
	sourceRsyncSocketPath = "/tmp/stunnel/rsync.sock"

	portCheckTimeout  = 30 * time.Second
	portCheckInterval = 1 * time.Second
	dialTimeout       = 1 * time.Second

	workerTypeSource      = "source"
	workerTypeDestination = "destination"
)

// StunnelConfig holds parameters for stunnel setup.
type StunnelConfig struct {
	WorkerType         string
	DestinationAddress string
	DestinationPort    string
	EnableRsyncTunnel  bool
	RsyncPort          string
	RsyncDaemonPort    string
}

// Stunnel manages the stunnel subprocess.
type Stunnel struct {
	config StunnelConfig
	pid    int
	logger logr.Logger
}

// NewStunnel creates a new Stunnel manager.
func NewStunnel(logger logr.Logger, cfg StunnelConfig) *Stunnel {
	return &Stunnel{
		config: cfg,
		logger: logger.WithName("stunnel"),
	}
}

// Start generates the config, starts stunnel, waits for
// PID file, and (for source) waits for ports to be ready.
func (s *Stunnel) Start() error {
	if err := s.generateConfig(); err != nil {
		return fmt.Errorf(
			"failed to generate stunnel config: %w", err,
		)
	}

	s.logger.Info("Starting stunnel")

	cmd := exec.Command(stunnelBin, stunnelConf)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"failed to start stunnel: %w", err,
		)
	}

	// Wait for stunnel to start and write PID file
	time.Sleep(2 * time.Second)

	if err := s.waitForPID(); err != nil {
		return err
	}

	s.logger.Info(
		"stunnel started successfully", "pid", s.pid,
	)

	if s.config.WorkerType == workerTypeSource {
		if err := s.waitForSockets(); err != nil {
			return err
		}
	}

	return nil
}

// Stop kills the stunnel process.
func (s *Stunnel) Stop() {
	if s.pid == 0 {
		return
	}

	s.logger.Info("Stopping stunnel", "pid", s.pid)

	proc, err := os.FindProcess(s.pid)
	if err != nil {
		s.logger.Error(
			err, "Failed to find stunnel process",
			"pid", s.pid,
		)
		return
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		s.logger.Error(
			err, "Failed to send SIGTERM to stunnel",
			"pid", s.pid,
		)
	}

	_, _ = proc.Wait()
}

// SourceGRPCAddress returns the local stunnel client
// endpoint address for source workers.
func (s *Stunnel) SourceGRPCAddress() string {
	return "unix://" + sourceGRPCSocketPath
}

func (s *Stunnel) generateConfig() error {
	if err := os.MkdirAll(stunnelDir, 0750); err != nil {
		return fmt.Errorf(
			"failed to create stunnel dir: %w", err,
		)
	}

	var b strings.Builder

	// Global options
	fmt.Fprintf(&b, "; Global options\n")
	fmt.Fprintf(&b, "pid = %s\n", stunnelPID)
	fmt.Fprintf(&b, "debug = 7\n")
	fmt.Fprintf(&b, "output = %s\n", stunnelLog)
	fmt.Fprintf(&b, "\n")
	fmt.Fprintf(&b, "; Use PSK (Pre-Shared Key) authentication\n")
	fmt.Fprintf(&b, "ciphers = PSK\n")
	fmt.Fprintf(&b, "PSKsecrets = %s\n", pskFilePath)
	fmt.Fprintf(&b, "\n")

	switch s.config.WorkerType {
	case workerTypeDestination:
		s.logger.Info(
			"Configuring stunnel as server (destination)",
		)
		s.writeDestinationConfig(&b)
	case workerTypeSource:
		s.logger.Info(
			"Configuring stunnel as client (source)",
		)
		s.writeSourceConfig(&b)
	default:
		return fmt.Errorf("unsupported worker type: %s", s.config.WorkerType)
	}

	s.logger.Info(
		"Writing stunnel config", "path", stunnelConf,
	)

	return os.WriteFile(
		stunnelConf, []byte(b.String()), 0600,
	)
}

func (s *Stunnel) writeDestinationConfig(
	b *strings.Builder,
) {
	// gRPC tunnel - server mode
	fmt.Fprintf(b, "; gRPC tunnel - for mover communication\n")
	fmt.Fprintf(b, "[grpc-tls]\n")
	fmt.Fprintf(b, "accept = %s\n", s.config.DestinationPort)
	fmt.Fprintf(
		b, "connect = 127.0.0.1:%s\n", wcommon.DefaultServerPort,
	)

	s.logger.Info("gRPC tunnel configured",
		"accept", s.config.DestinationPort,
		"connect", "127.0.0.1:"+wcommon.DefaultServerPort,
	)

	if s.config.EnableRsyncTunnel {
		fmt.Fprintf(b, "\n")
		fmt.Fprintf(b, "; Rsync tunnel - for rsync daemon communication\n")
		fmt.Fprintf(b, "[rsync-tls]\n")
		fmt.Fprintf(b, "accept = %s\n", s.config.RsyncPort)
		fmt.Fprintf(
			b, "connect = 127.0.0.1:%s\n",
			s.config.RsyncDaemonPort,
		)

		s.logger.Info("Rsync tunnel configured",
			"accept", s.config.RsyncPort,
			"connect", "127.0.0.1:"+s.config.RsyncDaemonPort,
		)
	}
}

func (s *Stunnel) writeSourceConfig(
	b *strings.Builder,
) {
	// gRPC tunnel - client mode
	fmt.Fprintf(b, "; gRPC tunnel - for mover communication\n")
	fmt.Fprintf(b, "[grpc-tls]\n")
	fmt.Fprintf(b, "client = yes\n")
	fmt.Fprintf(
		b, "accept = %s\n", sourceGRPCSocketPath,
	)
	fmt.Fprintf(
		b, "connect = %s:%s\n",
		s.config.DestinationAddress,
		s.config.DestinationPort,
	)

	s.logger.Info("gRPC tunnel configured",
		"accept", sourceGRPCSocketPath,
		"connect",
		s.config.DestinationAddress+":"+s.config.DestinationPort,
	)

	if s.config.EnableRsyncTunnel {
		fmt.Fprintf(b, "\n")
		fmt.Fprintf(b, "; Rsync tunnel - for rsync daemon communication\n")
		fmt.Fprintf(b, "[rsync-tls]\n")
		fmt.Fprintf(b, "client = yes\n")
		fmt.Fprintf(
			b, "accept = %s\n",
			sourceRsyncSocketPath,
		)
		fmt.Fprintf(
			b, "connect = %s:%s\n",
			s.config.DestinationAddress,
			s.config.RsyncPort,
		)

		s.logger.Info("Rsync tunnel configured",
			"accept", sourceRsyncSocketPath,
			"connect",
			s.config.DestinationAddress+":"+s.config.RsyncPort,
		)
	}
}

func (s *Stunnel) waitForPID() error {
	data, err := os.ReadFile(stunnelPID)
	if err != nil {
		s.logStunnelOutput()
		return fmt.Errorf(
			"stunnel failed to start - PID file not created: %w",
			err,
		)
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return fmt.Errorf(
			"failed to parse stunnel PID: %w", err,
		)
	}

	// Verify the process is running
	proc, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf(
			"stunnel process not found: %w", err,
		)
	}

	if err := proc.Signal(syscall.Signal(0)); err != nil {
		s.logStunnelOutput()
		return fmt.Errorf("stunnel process not running: %w", err)
	}

	s.pid = pid

	return nil
}

func (s *Stunnel) waitForSockets() error {
	s.logger.Info("Waiting for stunnel client sockets to be ready")

	deadline := time.Now().Add(portCheckTimeout)

	for time.Now().Before(deadline) {
		grpcReady := s.checkSocket(
			sourceGRPCSocketPath,
		)

		rsyncReady := true
		if s.config.EnableRsyncTunnel {
			rsyncReady = s.checkSocket(
				sourceRsyncSocketPath,
			)
		}

		if grpcReady && rsyncReady {
			s.logger.Info(
				"Stunnel client sockets are ready",
			)
			return nil
		}

		time.Sleep(portCheckInterval)
	}

	s.logStunnelOutput()

	return fmt.Errorf(
		"stunnel client sockets not ready after %v",
		portCheckTimeout,
	)
}

func (s *Stunnel) checkSocket(path string) bool {
	conn, err := net.DialTimeout("unix", path, dialTimeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func (s *Stunnel) logStunnelOutput() {
	data, err := os.ReadFile(stunnelLog)
	if err != nil {
		s.logger.Info("No stunnel log available")
		return
	}
	s.logger.Info("Stunnel log", "output", string(data))
}
