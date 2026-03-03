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
	"github.com/spf13/cobra"
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
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	var config Config

	cmd := &cobra.Command{
		Use:   "mover",
		Short: "Ceph VolSync Plugin Mover",
		Long: `A data mover component for the Ceph VolSync
Plugin that can operate as either a source or
destination worker.

This component handles data synchronization tasks
between storage systems as part of the Ceph VolSync
Plugin architecture.`,
		RunE: func(
			cmd *cobra.Command, args []string,
		) error {
			return runMover(config)
		},
	}

	// Add flags
	cmd.Flags().StringVar(
		&config.WorkerType, "worker-type", "",
		fmt.Sprintf(
			"Worker type (required): %s or %s",
			WorkerTypeSource, WorkerTypeDestination,
		),
	)
	cmd.Flags().StringVar(
		&config.DestinationAddress,
		"destination-address", "",
		"Destination address (optional, host:port)",
	)
	cmd.Flags().StringVar(
		&config.LogLevel, "log-level", "info",
		"Log level: debug, info, warn, error",
	)
	cmd.Flags().StringVar(
		&config.ServerPort, "server-port", "8080",
		"Port for gRPC server (default: 8080)",
	)

	// Mark required flags
	if err := cmd.MarkFlagRequired("worker-type"); err != nil {
		panic(fmt.Sprintf(
			"Failed to mark worker-type flag: %v", err,
		))
	}

	return cmd
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
	if err := validateWorkerType(
		config.WorkerType,
	); err != nil {
		return err
	}

	// Validate destination address if provided
	if config.DestinationAddress != "" {
		if err := validateDestinationAddress(
			config.DestinationAddress,
		); err != nil {
			return err
		}
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

func validateWorkerType(workerType string) error {
	if workerType != WorkerTypeSource &&
		workerType != WorkerTypeDestination {
		return fmt.Errorf(
			"invalid worker-type '%s': must be '%s' or '%s'",
			workerType,
			WorkerTypeSource,
			WorkerTypeDestination,
		)
	}
	return nil
}

func validateDestinationAddress(address string) error {
	if address == "" {
		return fmt.Errorf(
			"destination-address cannot be empty",
		)
	}
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
