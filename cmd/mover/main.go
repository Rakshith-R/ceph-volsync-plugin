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

	"github.com/RamenDR/ceph-volsync-plugin/internal/mover/destination"
	"github.com/RamenDR/ceph-volsync-plugin/internal/mover/rbd"
	"github.com/RamenDR/ceph-volsync-plugin/internal/mover/source"
	"github.com/RamenDR/ceph-volsync-plugin/internal/tunnel"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker"
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

	// Start the mover based on mover type and worker type
	switch config.MoverType {
	case "rbd":
		return runRBDMover(ctx, logger, config)
	case "cephfs":
		return runCephFSMover(ctx, logger, config)
	default:
		return fmt.Errorf(
			"invalid MOVER_TYPE '%s':"+
				" must be 'cephfs' or 'rbd'",
			config.MoverType,
		)
	}
}

func runCephFSMover(
	ctx context.Context,
	logger logr.Logger,
	config Config,
) error {
	switch config.WorkerType {
	case workerTypeSource:
		srcCfg := source.Config{
			DestinationAddress: config.DestinationAddress,
		}
		w := source.NewWorker(logger, srcCfg)
		return w.Run(ctx)
	case workerTypeDestination:
		dstCfg := destination.Config{
			ServerPort: config.ServerPort,
		}
		w := destination.NewWorker(logger, dstCfg)
		return w.Run(ctx)
	default:
		return fmt.Errorf(
			"invalid worker type: %s",
			config.WorkerType,
		)
	}
}

func runRBDMover(
	ctx context.Context,
	logger logr.Logger,
	config Config,
) error {
	switch config.WorkerType {
	case workerTypeSource:
		sourceConfig := rbd.SourceConfig{
			DestinationAddress: config.DestinationAddress,
		}
		worker := rbd.NewSourceWorker(
			logger, sourceConfig,
		)
		return worker.Run(ctx)
	case workerTypeDestination:
		destConfig := rbd.DestinationConfig{
			ServerPort: config.ServerPort,
		}
		worker := rbd.NewDestinationWorker(
			logger, destConfig,
		)
		return worker.Run(ctx)
	default:
		return fmt.Errorf(
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
