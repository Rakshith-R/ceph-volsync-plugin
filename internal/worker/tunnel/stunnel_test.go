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
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteSourceConfig_UsesUDS(t *testing.T) {
	// Clean up any existing config from previous tests
	_ = os.RemoveAll(stunnelDir)

	cfg := StunnelConfig{
		WorkerType:         "source",
		DestinationAddress: "10.0.0.1",
		DestinationPort:    "8000",
		EnableRsyncTunnel:  true,
		RsyncPort:          "8873",
		RsyncDaemonPort:    "8874",
	}

	s := NewStunnel(logr.Discard(), cfg)

	err := s.generateConfig()
	require.NoError(t, err, "generateConfig should succeed")

	// Read the generated config file
	content, err := os.ReadFile(stunnelConf)
	require.NoError(t, err, "should be able to read stunnel config")

	configStr := string(content)

	// Verify gRPC tunnel uses UDS
	assert.Contains(t, configStr, "accept = "+sourceGRPCSocketPath, "gRPC tunnel should use Unix socket path")
	assert.NotContains(t, configStr, "accept = 127.0.0.1:8001", "gRPC tunnel should not use TCP for source")

	// Verify rsync tunnel uses UDS
	assert.Contains(t, configStr, "accept = "+sourceRsyncSocketPath, "rsync tunnel should use Unix socket path")
	assert.NotContains(t, configStr, "accept = 127.0.0.1:8874", "rsync tunnel should not use TCP for source")

	// Verify client mode is set
	assert.Contains(t, configStr, "client = yes", "source config should have client = yes")

	// Verify connect directives point to destination
	assert.Contains(t, configStr, "connect = 10.0.0.1:8000", "gRPC tunnel should connect to destination")
	assert.Contains(t, configStr, "connect = 10.0.0.1:8873", "rsync tunnel should connect to destination")

	// Clean up
	_ = os.RemoveAll(stunnelDir)
}

func TestWriteDestinationConfig_StaysTCP(t *testing.T) {
	// Clean up any existing config from previous tests
	_ = os.RemoveAll(stunnelDir)

	cfg := StunnelConfig{
		WorkerType:         "destination",
		DestinationAddress: "0.0.0.0",
		DestinationPort:    "8000",
		EnableRsyncTunnel:  true,
		RsyncPort:          "8873",
		RsyncDaemonPort:    "8874",
	}

	s := NewStunnel(logr.Discard(), cfg)

	err := s.generateConfig()
	require.NoError(t, err, "generateConfig should succeed")

	// Read the generated config file
	content, err := os.ReadFile(stunnelConf)
	require.NoError(t, err, "should be able to read stunnel config")

	configStr := string(content)

	// Verify destination config uses TCP ports, not Unix sockets
	assert.Contains(t, configStr, "accept = 8000", "gRPC tunnel should use TCP port for destination")
	assert.NotContains(t, configStr, sourceGRPCSocketPath, "destination config should not use gRPC Unix socket path")

	// Verify rsync tunnel uses TCP port
	assert.Contains(t, configStr, "accept = 8873", "rsync tunnel should use TCP port for destination")
	assert.NotContains(t, configStr, sourceRsyncSocketPath, "destination config should not use rsync Unix socket path")

	// Verify server mode (no client = yes)
	assert.NotContains(t, configStr, "client = yes", "destination config should not have client = yes")

	// Verify connect directives point to localhost
	assert.Contains(t, configStr, "connect = 127.0.0.1:", "destination should connect to localhost services")

	// Clean up
	_ = os.RemoveAll(stunnelDir)
}

func TestSourceGRPCAddress_ReturnsUnixScheme(t *testing.T) {
	cfg := StunnelConfig{
		WorkerType:         "source",
		DestinationAddress: "10.0.0.1",
		DestinationPort:    "8000",
	}

	s := NewStunnel(logr.Discard(), cfg)

	addr := s.SourceGRPCAddress()

	expectedAddr := "unix://" + sourceGRPCSocketPath
	assert.Equal(t, expectedAddr, addr, "SourceGRPCAddress should return unix:// scheme with socket path")
	assert.True(t, strings.HasPrefix(addr, "unix://"), "address should have unix:// prefix")
	assert.Contains(t, addr, "/tmp/stunnel/grpc.sock", "address should contain the socket path")
}

func TestCheckSocket_ExistingSocket(t *testing.T) {
	tempDir := t.TempDir()
	socketPath := filepath.Join(tempDir, "test.sock")

	cfg := StunnelConfig{
		WorkerType:         "source",
		DestinationAddress: "10.0.0.1",
		DestinationPort:    "8000",
	}

	s := NewStunnel(logr.Discard(), cfg)

	// Socket should not exist initially
	exists := s.checkSocket(socketPath)
	assert.False(t, exists, "checkSocket should return false for nonexistent socket")

	// Create a Unix domain socket listener
	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err, "should be able to create Unix socket listener")
	defer func() { _ = listener.Close() }()

	// Socket should now exist and be connectable
	exists = s.checkSocket(socketPath)
	assert.True(t, exists, "checkSocket should return true for existing socket")

	// Close the listener
	_ = listener.Close()

	// Give the OS a moment to clean up
	// Note: On some systems, the socket file may still exist but not be connectable
	exists = s.checkSocket(socketPath)
	assert.False(t, exists, "checkSocket should return false after listener is closed")
}

func TestCheckSocket_NonexistentSocket(t *testing.T) {
	tempDir := t.TempDir()
	socketPath := filepath.Join(tempDir, "nonexistent.sock")

	cfg := StunnelConfig{
		WorkerType:         "source",
		DestinationAddress: "10.0.0.1",
		DestinationPort:    "8000",
	}

	s := NewStunnel(logr.Discard(), cfg)

	exists := s.checkSocket(socketPath)
	assert.False(t, exists, "checkSocket should return false for nonexistent socket")
}
