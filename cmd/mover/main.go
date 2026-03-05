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
	"github.com/RamenDR/ceph-volsync-plugin/internal/mover/source"
	"github.com/backube/volsync/controllers/utils"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	WorkerTypeSource      = "source"
	WorkerTypeDestination = "destination"
)

type Config struct {
	WorkerType         string
	DestinationAddress string
	LogLevel           string
	ServerPort         string
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
	return Config{
		WorkerType: os.Getenv("WORKER_TYPE"),
		DestinationAddress: os.Getenv(
			"DESTINATION_ADDRESS",
		),
		LogLevel:   envOrDefault("LOG_LEVEL", "info"),
		ServerPort: envOrDefault("SERVER_PORT", "8080"),
	}
}

func runMover(config Config) error {
	// Setup logging
	logger, err := setupLogger(config.LogLevel)
	if err != nil {
		return fmt.Errorf(
			"failed to setup logger: %w", err,
		)
	}

	ctrl.SetLogger(logger)

	// Set the SCC name in utils package
	utils.SCCName = "ceph-volsync-plugin-privileged-mover"

	// Validate worker type
	if config.WorkerType == "" {
		return fmt.Errorf(
			"WORKER_TYPE env var is required",
		)
	}

	if config.WorkerType != WorkerTypeSource &&
		config.WorkerType != WorkerTypeDestination {
		return fmt.Errorf(
			"invalid WORKER_TYPE '%s':"+
				" must be '%s' or '%s'",
			config.WorkerType,
			WorkerTypeSource,
			WorkerTypeDestination,
		)
	}

	logger.Info("Starting mover",
		"workerType", config.WorkerType,
		"destinationAddress",
		config.DestinationAddress,
	)

	// Setup graceful shutdown
	ctx, cancel := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT, syscall.SIGTERM,
	)
	defer cancel()

	// Start the mover based on worker type
	switch config.WorkerType {
	case WorkerTypeSource:
		sourceConfig := source.Config{
			DestinationAddress: config.DestinationAddress,
		}
		worker := source.NewWorker(logger, sourceConfig)
		return worker.Run(ctx)
	case WorkerTypeDestination:
		destConfig := destination.Config{
			ServerPort: config.ServerPort,
		}
		worker := destination.NewWorker(
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
