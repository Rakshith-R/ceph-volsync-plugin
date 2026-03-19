/*
Copyright 2025.

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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/RamenDR/ceph-volsync-plugin/internal/tunnel"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker"
	wcephfs "github.com/RamenDR/ceph-volsync-plugin/internal/worker/cephfs"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker/common"
	wrbd "github.com/RamenDR/ceph-volsync-plugin/internal/worker/rbd"
	"github.com/backube/volsync/controllers/utils"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	workerTypeSource      = "source"
	workerTypeDestination = "destination"
)

// Config holds configuration for the mover.
type Config struct {
	WorkerType         string
	MoverType          string
	DestinationAddress string
	LogLevel           string
	ServerPort         string
	DestinationPort    string
	EnableRsyncTunnel  bool
	RsyncPort          string
	RsyncDaemonPort    string
}

func main() {
	config := loadConfig()

	if err := runMover(config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return defaultVal
}

func loadConfig() Config {
	moverType := envOrDefault("MOVER_TYPE", "cephfs")
	return Config{
		WorkerType: os.Getenv(worker.EnvWorkerType),
		MoverType:  moverType,
		DestinationAddress: os.Getenv(
			worker.EnvDestinationAddress,
		),
		LogLevel: envOrDefault(
			worker.EnvLogLevel, "info",
		),
		ServerPort: envOrDefault(
			worker.EnvServerPort, "8080",
		),
		DestinationPort: envOrDefault(
			worker.EnvDestinationPort, "8000",
		),
		EnableRsyncTunnel: moverType == "cephfs",
		RsyncPort: envOrDefault(
			worker.EnvRsyncPort, "8873",
		),
		RsyncDaemonPort: envOrDefault(
			worker.EnvRsyncDaemonPort, "8874",
		),
	}
}

func runMover(config Config) error {
	logger, err := setupLogger(config.LogLevel)
	if err != nil {
		return fmt.Errorf(
			"failed to setup logger: %w", err,
		)
	}

	ctrl.SetLogger(logger)

	utils.SCCName = "ceph-volsync-plugin-privileged-mover"

	// Validate worker type
	if config.WorkerType == "" {
		return fmt.Errorf(
			"%s env var is required",
			worker.EnvWorkerType,
		)
	}

	if config.WorkerType != workerTypeSource &&
		config.WorkerType != workerTypeDestination {
		return fmt.Errorf(
			"invalid %s '%s': must be '%s' or '%s'",
			worker.EnvWorkerType,
			config.WorkerType,
			workerTypeSource,
			workerTypeDestination,
		)
	}

	// Setup tunnel infrastructure
	tunnelMgr := tunnel.NewManager(
		logger,
		tunnel.StunnelConfig{
			WorkerType:         config.WorkerType,
			DestinationAddress: config.DestinationAddress,
			DestinationPort:    config.DestinationPort,
			ServerPort:         config.ServerPort,
			EnableRsyncTunnel:  config.EnableRsyncTunnel,
			RsyncPort:          config.RsyncPort,
			RsyncDaemonPort:    config.RsyncDaemonPort,
		},
	)

	overriddenAddr, err := tunnelMgr.Setup()
	if err != nil {
		return fmt.Errorf(
			"failed to setup tunnel: %w", err,
		)
	}
	defer tunnelMgr.Cleanup()

	// Override destination address for source
	if overriddenAddr != "" {
		config.DestinationAddress = overriddenAddr
	}

	logger.Info("Starting mover",
		"workerType", config.WorkerType,
		"moverType", config.MoverType,
		"destinationAddress",
		config.DestinationAddress,
	)

	// Setup graceful shutdown
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGTERM,
	)
	defer cancel()

	// Create and run the appropriate worker
	w, err := newWorker(logger, config)
	if err != nil {
		return err
	}

	return w.Run(ctx)
}

// Runner is an interface for mover workers.
type Runner interface {
	Run(ctx context.Context) error
}

func newWorker(
	logger logr.Logger, config Config,
) (Runner, error) {
	switch config.MoverType {
	case "cephfs":
		return newCephFSWorker(logger, config)
	case "rbd":
		return newRBDWorker(logger, config)
	default:
		return nil, fmt.Errorf(
			"invalid MOVER_TYPE '%s':"+
				" must be 'cephfs' or 'rbd'",
			config.MoverType,
		)
	}
}

func newCephFSWorker(
	logger logr.Logger, config Config,
) (Runner, error) {
	switch config.WorkerType {
	case workerTypeSource:
		return wcephfs.NewSourceWorker(
			logger,
			common.SourceConfig{
				DestinationAddress: config.DestinationAddress,
			},
		), nil
	case workerTypeDestination:
		return wcephfs.NewDestinationWorker(
			logger,
			common.DestinationConfig{
				ServerPort: config.ServerPort,
			},
		), nil
	default:
		return nil, fmt.Errorf(
			"invalid worker type: %s",
			config.WorkerType,
		)
	}
}

func newRBDWorker(
	logger logr.Logger, config Config,
) (Runner, error) {
	switch config.WorkerType {
	case workerTypeSource:
		return wrbd.NewSourceWorker(
			logger,
			common.SourceConfig{
				DestinationAddress: config.DestinationAddress,
			},
		), nil
	case workerTypeDestination:
		return wrbd.NewDestinationWorker(
			logger,
			common.DestinationConfig{
				ServerPort: config.ServerPort,
			},
		), nil
	default:
		return nil, fmt.Errorf(
			"invalid worker type: %s",
			config.WorkerType,
		)
	}
}

func setupLogger(level string) (logr.Logger, error) {
	var zapLevel zapcore.Level

	switch level {
	case "debug":
		zapLevel = zapcore.DebugLevel
	case "info":
		zapLevel = zapcore.InfoLevel
	case "warn":
		zapLevel = zapcore.WarnLevel
	case "error":
		zapLevel = zapcore.ErrorLevel
	default:
		return logr.Logger{}, fmt.Errorf(
			"invalid log level: %s", level,
		)
	}

	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zapLevel)

	zapLogger, err := zapConfig.Build()
	if err != nil {
		return logr.Logger{}, fmt.Errorf(
			"failed to build zap logger: %w", err,
		)
	}

	logger := zapr.NewLogger(zapLogger)

	return logger.WithName("mover").WithValues(
		"component", "mover",
	), nil
}
