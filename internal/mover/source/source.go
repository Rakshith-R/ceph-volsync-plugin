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

package source

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/version/v1"
	"github.com/RamenDR/ceph-volsync-plugin/internal/worker"
)

const (
	// connectionTimeout is the timeout for the first
	// RPC call which establishes the connection.
	connectionTimeout = 60 * time.Second

	// rpcTimeout is the timeout for subsequent RPC
	// calls after connection is established.
	rpcTimeout = 30 * time.Second

	maxRetries    = 5
	initialDelay  = 2 * time.Second
	backoffFactor = 2
	sourceDir     = "/data"
	fileListPath  = "/tmp/filelist.txt"
)

// Config holds configuration for the source worker.
type Config struct {
	DestinationAddress string
}

// Worker represents a source worker instance.
type Worker struct {
	logger logr.Logger
	config Config
}

// NewWorker creates a new source worker.
func NewWorker(
	logger logr.Logger, config Config,
) *Worker {
	return &Worker{
		logger: logger.WithName("source-worker"),
		config: config,
	}
}

// Run starts the source worker.
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("Starting source worker")

	if w.config.DestinationAddress == "" {
		return fmt.Errorf(
			"%s is required for source worker",
			worker.EnvDestinationAddress,
		)
	}

	w.logger.Info("Connecting to destination",
		"address", w.config.DestinationAddress,
	)

	conn, err := grpc.NewClient(
		w.config.DestinationAddress,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to create gRPC client: %w", err,
		)
	}
	defer conn.Close()

	// Call GetVersion to verify connectivity
	versionClient := versionv1.NewVersionServiceClient(conn)

	callCtx, cancel := context.WithTimeout(
		ctx, connectionTimeout,
	)
	defer cancel()

	w.logger.Info(
		"Calling GetVersion on destination",
		"establishingConnection", true,
	)

	resp, err := versionClient.GetVersion(
		callCtx, &versionv1.GetVersionRequest{},
	)
	if err != nil {
		w.logger.Error(err,
			"Failed to get version from destination",
			"address", w.config.DestinationAddress,
		)
		return fmt.Errorf(
			"failed to get version from destination %s: %w",
			w.config.DestinationAddress, err,
		)
	}

	w.logger.Info("Retrieved version from destination",
		"version", resp.GetVersion(),
	)

	// Run rsync synchronization
	if err := w.rsync(); err != nil {
		w.logger.Error(err, "rsync failed")
		return fmt.Errorf("rsync failed: %w", err)
	}

	// Signal completion
	doneClient := apiv1.NewDoneServiceClient(conn)

	doneCtx, doneCancel := context.WithTimeout(
		ctx, rpcTimeout,
	)
	defer doneCancel()

	_, err = doneClient.Done(
		doneCtx, &apiv1.DoneRequest{},
	)
	if err != nil {
		w.logger.Error(
			err,
			"Failed to send Done signal to destination",
		)
		return fmt.Errorf(
			"failed to send Done signal: %w", err,
		)
	}

	w.logger.Info(
		"Successfully sent Done signal to destination",
	)

	return nil
}

// rsync performs two-pass rsync with retry logic.
func (w *Worker) rsync() error {
	startTime := time.Now()
	rsyncDaemonPort := os.Getenv(
		worker.EnvRsyncDaemonPort,
	)
	rsyncTarget := fmt.Sprintf(
		"rsync://127.0.0.1:%s/data", rsyncDaemonPort,
	)

	w.logger.Info("Starting rsync synchronization",
		"target", rsyncTarget,
	)

	retry := 0
	delay := initialDelay
	rc := 1

	for rc != 0 && retry < maxRetries {
		retry++
		w.logger.Info("Rsync attempt",
			"retry", retry, "maxRetries", maxRetries,
		)

		rcA, err := w.createFileListAndSync(rsyncTarget)
		if err != nil {
			w.logger.Error(err,
				"Failed to create file list or sync",
				"retry", retry,
			)
		}

		// Delay between passes to let rsync daemon
		// clean up the connection.
		if rcA == 0 {
			time.Sleep(1 * time.Second)
		}

		rcB := w.syncForDeletion(rsyncTarget)

		rc = rcA*100 + rcB

		if rc != 0 {
			if retry < maxRetries {
				w.logger.Info(
					"Synchronization failed, retrying",
					"delay", delay,
					"retry", retry,
					"maxRetries", maxRetries,
					"returnCode", rc,
				)
				time.Sleep(delay)
				delay = delay * backoffFactor
			} else {
				w.logger.Error(
					fmt.Errorf("rsync code %d", rc),
					"Synchronization failed after all retries",
				)
			}
		}
	}

	duration := time.Since(startTime)
	w.logger.Info("Rsync completed",
		"durationSeconds", duration.Seconds(),
		"returnCode", rc,
	)

	if rc != 0 {
		return fmt.Errorf(
			"synchronization failed after %d retries,"+
				" rsync returned: %d",
			maxRetries, rc,
		)
	}

	w.logger.Info("Synchronization successful")

	return nil
}

// createFileListAndSync generates the file list and
// performs the first rsync pass.
func (w *Worker) createFileListAndSync(
	rsyncTarget string,
) (int, error) {
	findCmd := exec.Command(
		"sh", "-c",
		fmt.Sprintf(
			"find %s -mindepth 1 -maxdepth 1"+
				" | sed 's|^%s|/|'",
			sourceDir, sourceDir,
		),
	)

	output, err := findCmd.Output()
	if err != nil {
		return 1, fmt.Errorf(
			"failed to list source directory: %w", err,
		)
	}

	if err := os.WriteFile(
		fileListPath, output, 0644,
	); err != nil {
		return 1, fmt.Errorf(
			"failed to write file list: %w", err,
		)
	}

	fileInfo, err := os.Stat(fileListPath)
	if err != nil {
		return 1, fmt.Errorf(
			"failed to stat file list: %w", err,
		)
	}

	if fileInfo.Size() == 0 {
		w.logger.Info(
			"Skipping sync of empty source directory",
		)
		return 0, nil
	}

	rsyncArgs := []string{
		"-aAhHSxz",
		"-r",
		"--exclude=lost+found",
		"--itemize-changes",
		"--info=stats2,misc2",
		"--files-from=" + fileListPath,
		sourceDir + "/",
		rsyncTarget,
	}

	w.logger.Info(
		"Running first rsync pass (file preservation)",
		"args", strings.Join(rsyncArgs, " "),
	)

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.logger.Info("First rsync pass failed",
				"exitCode", exitErr.ExitCode(),
			)
			return exitErr.ExitCode(), nil
		}
		w.logger.Error(
			err, "Failed to execute first rsync pass",
		)
		return 1, err
	}

	w.logger.Info("First rsync pass completed successfully")

	return 0, nil
}

// syncForDeletion performs the second rsync pass to
// delete extra files on destination.
func (w *Worker) syncForDeletion(
	rsyncTarget string,
) int {
	rsyncArgs := []string{
		"-rx",
		"--exclude=lost+found",
		"--ignore-existing",
		"--ignore-non-existing",
		"--delete",
		"--itemize-changes",
		"--info=stats2,misc2",
		sourceDir + "/",
		rsyncTarget,
	}

	w.logger.Info(
		"Running second rsync pass (deletion)",
		"args", strings.Join(rsyncArgs, " "),
	)

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.logger.Info("Second rsync pass failed",
				"exitCode", exitErr.ExitCode(),
			)
			return exitErr.ExitCode()
		}
		w.logger.Error(
			err, "Failed to execute second rsync pass",
		)
		return 1
	}

	w.logger.Info(
		"Second rsync pass completed successfully",
	)

	return 0
}
