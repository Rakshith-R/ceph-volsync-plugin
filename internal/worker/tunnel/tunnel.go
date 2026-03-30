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

	"github.com/go-logr/logr"
)

// Manager orchestrates stunnel and optional rsync
// daemon lifecycle.
type Manager struct {
	stunnel     *Stunnel
	rsyncDaemon *RsyncDaemon
	logger      logr.Logger
	config      StunnelConfig
}

// NewManager creates a tunnel manager from config.
func NewManager(
	logger logr.Logger, cfg StunnelConfig,
) *Manager {
	return &Manager{
		logger: logger.WithName("tunnel"),
		config: cfg,
	}
}

// Setup validates prerequisites, starts stunnel, and
// conditionally starts rsync daemon. For source
// workers, returns the overridden destination address
// pointing to the local stunnel client endpoint.
func (m *Manager) Setup() (string, error) {
	// Validate PSK file exists
	if _, err := os.Stat(pskFilePath); err != nil {
		return "", fmt.Errorf(
			"PSK file not found at %s: %w",
			pskFilePath, err,
		)
	}

	if m.config.EnableRsyncTunnel {
		m.logger.Info(
			"Starting dual stunnel setup (gRPC + rsync)",
			"workerType", m.config.WorkerType,
		)
	} else {
		m.logger.Info(
			"Starting stunnel setup (gRPC only)",
			"workerType", m.config.WorkerType,
		)
	}

	// Start stunnel
	m.stunnel = NewStunnel(m.logger, m.config)

	if err := m.stunnel.Start(); err != nil {
		return "", fmt.Errorf(
			"failed to start stunnel: %w", err,
		)
	}

	// Start rsync daemon on destination if enabled
	if m.config.WorkerType == workerTypeDestination &&
		m.config.EnableRsyncTunnel {
		m.rsyncDaemon = NewRsyncDaemon(
			m.logger, m.config.RsyncDaemonPort,
		)

		if err := m.rsyncDaemon.Start(); err != nil {
			m.stunnel.Stop()
			return "", fmt.Errorf(
				"failed to start rsync daemon: %w", err,
			)
		}
	}

	// For source, return the local stunnel client
	// endpoint as the overridden destination address.
	if m.config.WorkerType == workerTypeSource {
		return m.stunnel.SourceGRPCAddress(), nil
	}

	return "", nil
}

// Cleanup stops rsync daemon (if running) and stunnel.
// Safe to call multiple times.
func (m *Manager) Cleanup() {
	m.logger.Info("Cleaning up tunnel processes")

	if m.rsyncDaemon != nil {
		m.rsyncDaemon.Stop()
	}

	if m.stunnel != nil {
		m.stunnel.Stop()
	}
}
