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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	WorkerTypeSource      = "source"
	WorkerTypeDestination = "destination"

	// ConfigMap data keys
	CsiConfigMapConfigKey  = "config.json"
	CsiConfigMapMappingKey = "cluster-mapping.json"

	// Default paths for writing config files
	defaultConfigDir       = "/etc/ceph-csi-config"
	defaultConfigFilePath  = "/etc/ceph-csi-config/config.json"
	defaultMappingFilePath = "/etc/ceph-csi-config/cluster-mapping.json"
	podNamespaceEnvVar     = "POD_NAMESPACE"
)

type Config struct {
	WorkerType         string
	DestinationAddress string
	LogLevel           string
	ServerPort         string
	ConfigMapName      string
	ConfigMapNamespace string
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
		Long: `A data mover component for the Ceph VolSync Plugin that can operate as either a source or destination worker.
		
This component handles data synchronization tasks between storage systems as part of the
Ceph VolSync Plugin architecture.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return runMover(config)
		},
	}

	// Add flags
	cmd.Flags().StringVar(&config.WorkerType, "worker-type", "",
		fmt.Sprintf("Worker type (required): %s or %s", WorkerTypeSource, WorkerTypeDestination))
	cmd.Flags().StringVar(&config.DestinationAddress, "destination-address", "",
		"Destination address for data transfer (optional, format: host:port)")
	cmd.Flags().StringVar(&config.LogLevel, "log-level", "info",
		"Log level: debug, info, warn, error")
	cmd.Flags().StringVar(&config.ServerPort, "server-port", "8080",
		"Port for gRPC server (default: 8080)")
	cmd.Flags().StringVar(&config.ConfigMapName, "configmap-name", "",
		"Name of the ConfigMap containing Ceph CSI configuration")
	cmd.Flags().StringVar(&config.ConfigMapNamespace, "configmap-namespace", "",
		"Namespace of the ConfigMap (defaults to POD_NAMESPACE env var if not specified)")

	// Mark required flags
	if err := cmd.MarkFlagRequired("worker-type"); err != nil {
		panic(fmt.Sprintf("Failed to mark worker-type flag as required: %v", err))
	}

	return cmd
}

func runMover(config Config) error {
	// Setup logging
	logger, err := setupLogger(config.LogLevel)
	if err != nil {
		return fmt.Errorf("failed to setup logger: %w", err)
	}

	ctrl.SetLogger(logger)

	// Set the SCC name in utils package for use in Role creation
	utils.SCCName = "ceph-volsync-plugin-privileged-mover"

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

	// Create in-cluster config
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("failed to create in-cluster config: %w", err)
	}

	// Create Kubernetes clientset
	clientset, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	// Process ConfigMap configuration if configmap-name is provided
	if err := fetchAndWriteConfigMapData(clientset, logger, config.ConfigMapName, config.ConfigMapNamespace); err != nil {
		return fmt.Errorf("failed to fetch and write ConfigMap data: %w", err)
	}
	logger.Info("Successfully fetched and wrote ConfigMap data",
		"configMapName", config.ConfigMapName,
		"configMapNamespace", config.ConfigMapNamespace)

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
			Clientset:          clientset,
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
		// This should never happen due to validation, but included for completeness
		return fmt.Errorf("invalid worker type: %s", config.WorkerType)
	}
}

func validateWorkerType(workerType string) error {
	if workerType != WorkerTypeSource && workerType != WorkerTypeDestination {
		return fmt.Errorf("invalid worker-type '%s': must be '%s' or '%s'",
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

// fetchAndWriteConfigMapData fetches the ConfigMap from k8s and writes its data entries to files.
func fetchAndWriteConfigMapData(clientset *kubernetes.Clientset, logger logr.Logger, configMapName, configMapNamespace string) error {
	// Validate configmap-name is not empty
	if configMapName == "" {
		return fmt.Errorf("configmap-name cannot be empty")
	}

	// Set configmap-namespace to POD namespace if it is empty
	namespace := configMapNamespace
	if namespace == "" {
		namespace = os.Getenv(podNamespaceEnvVar)
		if namespace == "" {
			return fmt.Errorf("configmap-namespace is empty and %s environment variable is not set", podNamespaceEnvVar)
		}
		logger.Info("Using POD namespace for ConfigMap", "namespace", namespace)
	}

	// Fetch the ConfigMap
	logger.Info("Fetching ConfigMap", "name", configMapName, "namespace", namespace)
	configMap, err := clientset.CoreV1().ConfigMaps(namespace).Get(context.Background(), configMapName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch ConfigMap %s/%s: %w", namespace, configMapName, err)
	}

	// Create the config directory if it doesn't exist
	if err := os.MkdirAll(defaultConfigDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory %s: %w", defaultConfigDir, err)
	}

	// Write config.json if present in ConfigMap
	if configData, ok := configMap.Data[CsiConfigMapConfigKey]; ok {
		configPath := defaultConfigFilePath
		logger.Info("Writing config file", "key", CsiConfigMapConfigKey, "path", configPath)
		if err := os.WriteFile(configPath, []byte(configData), 0644); err != nil {
			return fmt.Errorf("failed to write config file %s: %w", configPath, err)
		}
		logger.Info("Successfully wrote config file", "path", configPath)
	} else {
		logger.Info("ConfigMap does not contain key", "key", CsiConfigMapConfigKey)
	}

	// Write cluster-mapping.json if present in ConfigMap
	if mappingData, ok := configMap.Data[CsiConfigMapMappingKey]; ok {
		mappingPath := defaultMappingFilePath
		logger.Info("Writing cluster mapping file", "key", CsiConfigMapMappingKey, "path", mappingPath)
		if err := os.WriteFile(mappingPath, []byte(mappingData), 0644); err != nil {
			return fmt.Errorf("failed to write cluster mapping file %s: %w", mappingPath, err)
		}
		logger.Info("Successfully wrote cluster mapping file", "path", mappingPath)
	} else {
		logger.Info("ConfigMap does not contain key", "key", CsiConfigMapMappingKey)
	}

	return nil
}
