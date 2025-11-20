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
	if err := runMover(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runMover() error {
	// Load configuration from environment variables
	config := Config{
		WorkerType:         getEnvOrDefault("WORKER_TYPE", ""),
		DestinationAddress: getEnvOrDefault("DESTINATION_ADDRESS", ""),
		LogLevel:           getEnvOrDefault("LOG_LEVEL", "info"),
		ServerPort:         getEnvOrDefault("SERVER_PORT", "8080"),
	}

	// Setup logging
	logger, err := setupLogger(config.LogLevel)
	if err != nil {
		return fmt.Errorf("failed to setup logger: %w", err)
	}

	ctrl.SetLogger(logger)

	// Validate worker type
	if err := validateWorkerType(config.WorkerType); err != nil {
		return err
	}

	// Validate destination address if provided
	if config.DestinationAddress != "" {
		if err := validateDestinationAddress(config.DestinationAddress); err != nil {
			return err
		}
	}

	logger.Info("Starting mover",
		"workerType", config.WorkerType,
		"destinationAddress", config.DestinationAddress)

	// Setup graceful shutdown
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
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
		worker := destination.NewWorker(logger, destConfig)
		return worker.Run(ctx)
	default:
		return fmt.Errorf("invalid worker type: %s. Must be set via WORKER_TYPE environment variable to '%s' or '%s'",
			config.WorkerType, WorkerTypeSource, WorkerTypeDestination)
	}
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func validateWorkerType(workerType string) error {
	if workerType != WorkerTypeSource && workerType != WorkerTypeDestination {
		return fmt.Errorf("invalid worker-type '%s': must be '%s' or '%s'. Set via WORKER_TYPE environment variable",
			workerType, WorkerTypeSource, WorkerTypeDestination)
	}
	return nil
}

func validateDestinationAddress(address string) error {
	if address == "" {
		return fmt.Errorf("destination-address cannot be empty")
	}
	// TODO: Add more sophisticated address validation (host:port format, reachability, etc.)
	return nil
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
		return logr.Logger{}, fmt.Errorf("invalid log level: %s", level)
	}

	zapConfig := zap.NewProductionConfig()
	zapConfig.Level = zap.NewAtomicLevelAt(zapLevel)

	zapLogger, err := zapConfig.Build()
	if err != nil {
		return logr.Logger{}, fmt.Errorf("failed to build zap logger: %w", err)
	}

	// Convert zap logger to logr using zapr
	logger := zapr.NewLogger(zapLogger)

	return logger.WithName("mover").WithValues("component", "mover"), nil
}
