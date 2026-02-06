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
	"log"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/ceph/go-ceph/cephfs"
	ca "github.com/ceph/go-ceph/cephfs/admin"

	"github.com/RamenDR/ceph-volsync-plugin/internal/ceph"
	apiv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/api/v1"
	versionv1 "github.com/RamenDR/ceph-volsync-plugin/internal/mover/proto/version/v1"
)

const (
	// connectionTimeout is the timeout for the first RPC call which establishes the connection.
	// This is longer because it includes DNS resolution, TCP handshake, and TLS negotiation.
	connectionTimeout = 60 * time.Second

	// rpcTimeout is the timeout for subsequent RPC calls after connection is established.
	rpcTimeout = 30 * time.Second

	// rsync configuration constants
	maxRetries    = 5
	initialDelay  = 2 * time.Second
	backoffFactor = 2
	sourceDir     = "/data"
	fileListPath  = "/tmp/filelist.txt"
)

// snapDiffResult combines a snap diff entry and error for simplified channel communication.
// When Err is non-nil, Entry should be ignored. When Err is nil and Entry is nil,
// processing is complete.
type snapDiffResult struct {
	Entry *cephfs.SnapDiffEntry
	Err   error
}

// Config holds configuration for the source worker
type Config struct {
	DestinationAddress string
	Clientset          *kubernetes.Clientset
}

// Worker represents a source worker instance
type Worker struct {
	logger logr.Logger
	config Config
}

// NewWorker creates a new source worker
func NewWorker(logger logr.Logger, config Config) *Worker {
	return &Worker{
		logger: logger.WithName("source-worker"),
		config: config,
	}
}

// Run starts the source worker
func (w *Worker) Run(ctx context.Context) error {
	w.logger.Info("Starting source worker")

	if w.config.DestinationAddress == "" {
		w.logger.Info("No destination address provided, running without version checks")
		<-ctx.Done()
		w.logger.Info("Source worker shutting down")
		return ctx.Err()
	}

	w.logger.Info("Connecting to destination", "address", w.config.DestinationAddress)

	// Create gRPC connection (non-blocking, connection established lazily)
	conn, err := grpc.NewClient(
		w.config.DestinationAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client for destination %s: %w", w.config.DestinationAddress, err)
	}
	defer conn.Close()

	// Create version service client
	versionClient := versionv1.NewVersionServiceClient(conn)

	// Call GetVersion with longer timeout to allow for connection establishment
	// The first RPC call will trigger connection establishment, so we need more time
	callCtx, cancel := context.WithTimeout(ctx, connectionTimeout)
	defer cancel()

	w.logger.Info("Calling GetVersion on destination", "establishingConnection", true)

	resp, err := versionClient.GetVersion(callCtx, &versionv1.GetVersionRequest{})
	if err != nil {
		w.logger.Error(err, "Failed to get version from destination",
			"address", w.config.DestinationAddress,
			"hint", "Ensure the destination service is running and accessible")
		return fmt.Errorf("failed to get version from destination %s: %w", w.config.DestinationAddress, err)
	}
	w.logger.Info("Retrieved version from destination", "version", resp.GetVersion())

	baseSnapshotHandle := os.Getenv("BASE_SNAPSHOT_HANDLE")
	targetSnapshotHandle := os.Getenv("TARGET_SNAPSHOT_HANDLE")
	volumeHandle := os.Getenv("VOLUME_HANDLE")
	if baseSnapshotHandle == "" || targetSnapshotHandle == "" || volumeHandle == "" {
		w.logger.Info("Snapshot handles not set, using rsync on /data")
		// just use rsync on /data
		err := w.rsync()
		if err != nil {
			w.logger.Error(err, "rsync failed")
			return fmt.Errorf("rsync failed: %w", err)
		}

		// Create done service client and signal completion
		doneClient := apiv1.NewDoneServiceClient(conn)
		doneCtx, doneCancel := context.WithTimeout(ctx, rpcTimeout)
		defer doneCancel()

		_, err = doneClient.Done(doneCtx, &apiv1.DoneRequest{})
		if err != nil {
			w.logger.Error(err, "Failed to send Done signal to destination")
			return fmt.Errorf("failed to send Done signal to destination: %w", err)
		}
		w.logger.Info("Successfully sent Done signal to destination")
		return nil
	}
	//decode
	baseSnapID := &ceph.CSIIdentifier{}
	err = baseSnapID.DecomposeCSIID(baseSnapshotHandle)
	if err != nil {
		w.logger.Error(err, "Failed to decompose BASE_SNAPSHOT_HANDLE")
		return fmt.Errorf("failed to decompose BASE_SNAPSHOT_HANDLE: %w", err)
	}
	targetSnapID := &ceph.CSIIdentifier{}
	err = targetSnapID.DecomposeCSIID(targetSnapshotHandle)
	if err != nil {
		w.logger.Error(err, "Failed to decompose TARGET_SNAPSHOT_HANDLE")
		return fmt.Errorf("failed to decompose TARGET_SNAPSHOT_HANDLE: %w", err)
	}

	volumeID := &ceph.CSIIdentifier{}
	err = volumeID.DecomposeCSIID(volumeHandle)
	if err != nil {
		w.logger.Error(err, "Failed to decompose VOLUME_HANDLE")
		return fmt.Errorf("failed to decompose VOLUME_HANDLE: %w", err)
	}
	subVolumeGroup, err := ceph.CephFSSubvolumeGroup(ceph.CsiConfigFile, volumeID.ClusterID)
	if err != nil {
		w.logger.Error(err, "Failed to get subvolume group")
		return fmt.Errorf("failed to get subvolume group: %w", err)
	}
	subVolumeName := "csi-vol-" + volumeID.ObjectUUID
	baseSnapName := "csi-snap-" + baseSnapID.ObjectUUID
	targetSnapName := "csi-snap-" + targetSnapID.ObjectUUID

	secretName, secretNamespace, err := ceph.GetCephFSControllerPublishSecretRef(ceph.CsiConfigFile, baseSnapID.ClusterID)
	if err != nil {
		w.logger.Error(err, "Failed to get secret ref for BASE_SNAPSHOT_HANDLE")
		return fmt.Errorf("failed to get secret ref for BASE_SNAPSHOT_HANDLE: %w", err)
	}

	// get k8s secret
	secret, err := w.config.Clientset.CoreV1().Secrets(secretNamespace).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		w.logger.Error(err, "Failed to get secret for BASE_SNAPSHOT_HANDLE")

		return fmt.Errorf("failed to get secret for BASE_SNAPSHOT_HANDLE: %w", err)
	}
	data := map[string]string{}
	for k, v := range secret.Data {
		data[k] = string(v)
	}
	creds, err := ceph.NewAdminCredentials(data)
	if err != nil {
		return fmt.Errorf("failed to get creds %v:%w", data, err)
	}

	mons, err := ceph.Mons(ceph.CsiConfigFile, volumeID.ClusterID)
	if err != nil {
		return fmt.Errorf("failed to mons: %w", data)
	}

	cc := &ceph.ClusterConnection{}
	if err := cc.Connect(mons, creds); err != nil {
		return fmt.Errorf("failed to connect to cluster: %w", err)
	}
	defer cc.Destroy()

	// Get FSAdmin and subvolume path BEFORE mounting
	// This is necessary because a restricted ceph user may only have access
	// to the specific subvolume path, not the root of the filesystem
	fsa, err := cc.GetFSAdmin()
	if err != nil {
		return fmt.Errorf("failed to get FSAdmin: %w", err)
	}

	volume, err := getFSName(fsa, volumeID.LocationID)
	if err != nil {
		return err
	}
	subVolumePath, err := fsa.SubVolumePath(volume, subVolumeGroup, subVolumeName)
	if err != nil {
		return fmt.Errorf("failed to get subvolume path: %w", err)
	}

	mountInfo, err := cc.CreateMountFromRados()
	if err != nil {
		return fmt.Errorf("failed to create cephfs from rados: %w", err)
	}
	// Mount directly at the subvolume path to support restricted ceph users
	// who only have access to this specific subvolume
	err = mountInfo.MountWithRoot(subVolumePath)
	if err != nil {
		return fmt.Errorf("failed to mount at subvolume path %s: %w", subVolumePath, err)
	}
	// Since we mounted with MountWithRoot(subVolumePath), the root of the mount
	// is now the subvolume itself. Therefore:
	// - rootPath should be "/" (the root of our mounted filesystem)
	// - relPath should be "." (we start from the root of the mount)
	rootPath := "/"
	relPath := "."
	log.Default().Printf("Mounted at subvolume path: %s, using rootPath: %s, relPath: %s\n", subVolumePath, rootPath, relPath)

	dataVolumePath := "/data"
	resultChan := initSnapDiffChan(mountInfo, dataVolumePath, rootPath, relPath, baseSnapName, targetSnapName)

	// Process snap diff results from the unified channel
	if err := processSnapDiffResults(ctx, resultChan, relPath, dataVolumePath); err != nil {
		return err
	}

	// Create done service client
	doneClient := apiv1.NewDoneServiceClient(conn)

	// Call Done to signal completion and request graceful shutdown
	// Connection is already established, so use shorter timeout
	doneCtx, doneCancel := context.WithTimeout(ctx, rpcTimeout)
	defer doneCancel()

	_, err = doneClient.Done(doneCtx, &apiv1.DoneRequest{})
	if err != nil {
		w.logger.Error(err, "Failed to send Done signal to destination")
		return fmt.Errorf("failed to send Done signal to destination: %w", err)
	}
	w.logger.Info("Successfully sent Done signal to destination")

	return nil

}

// rsync performs the rsync synchronization with retry logic
// This implements the bash script logic from the comments above
func (w *Worker) rsync() error {
	startTime := time.Now()
	rsyncDaemonPort := os.Getenv("RSYNC_DAEMON_PORT")
	rsyncTarget := fmt.Sprintf("rsync://127.0.0.1:%s/data", rsyncDaemonPort)

	w.logger.Info("Starting rsync synchronization", "target", rsyncTarget)

	retry := 0
	delay := initialDelay
	rc := 1 // Non-zero to enter loop

	for rc != 0 && retry < maxRetries {
		retry++
		w.logger.Info("Rsync attempt", "retry", retry, "maxRetries", maxRetries)

		// Create file list of all items at root of PVC
		rcA, err := w.createFileListAndSync(rsyncTarget)
		if err != nil {
			w.logger.Error(err, "Failed to create file list or sync", "retry", retry)
		}

		// Small delay between rsync passes to allow the rsync daemon to clean up the connection
		// This prevents "Connection reset by peer" errors when the second pass starts
		// immediately after the first pass completes
		if rcA == 0 {
			time.Sleep(1 * time.Second)
		}

		// Second pass: delete extra files on destination
		rcB := w.syncForDeletion(rsyncTarget)

		// Combine return codes: rc = rc_a * 100 + rc_b
		rc = rcA*100 + rcB

		if rc != 0 {
			if retry < maxRetries {
				w.logger.Info("Synchronization failed, retrying",
					"delay", delay,
					"retry", retry,
					"maxRetries", maxRetries,
					"returnCode", rc)
				time.Sleep(delay)
				delay = delay * backoffFactor
			} else {
				w.logger.Error(fmt.Errorf("rsync failed with code %d", rc),
					"Synchronization failed after all retries")
			}
		}
	}

	duration := time.Since(startTime)
	w.logger.Info("Rsync completed", "durationSeconds", duration.Seconds(), "returnCode", rc)

	if rc != 0 {
		return fmt.Errorf("synchronization failed after %d retries, rsync returned: %d", maxRetries, rc)
	}

	w.logger.Info("Synchronization successful")
	return nil
}

// createFileListAndSync generates the file list and performs the first rsync pass
func (w *Worker) createFileListAndSync(rsyncTarget string) (int, error) {
	// Find all files/dirs at root of pvc, prepend / to each
	// BusyBox find doesn't support -printf, so use find + sed
	// This matches: find "${SOURCE}" -mindepth 1 -maxdepth 1 | sed "s|^${SOURCE}|/|" > /tmp/filelist.txt
	findCmd := exec.Command("sh", "-c", fmt.Sprintf("find %s -mindepth 1 -maxdepth 1 | sed 's|^%s|/|'", sourceDir, sourceDir))

	output, err := findCmd.Output()
	if err != nil {
		return 1, fmt.Errorf("failed to list source directory: %w", err)
	}

	// Write file list to temp file
	if err := os.WriteFile(fileListPath, output, 0644); err != nil {
		return 1, fmt.Errorf("failed to write file list: %w", err)
	}

	// Check if file list is empty
	fileInfo, err := os.Stat(fileListPath)
	if err != nil {
		return 1, fmt.Errorf("failed to stat file list: %w", err)
	}

	if fileInfo.Size() == 0 {
		w.logger.Info("Skipping sync of empty source directory")
		return 0, nil
	}

	// First rsync pass: preserve as much as possible, exclude root directory
	// rsync -aAhHSxz -r --exclude=lost+found --itemize-changes --info=stats2,misc2 --files-from=/tmp/filelist.txt ${SOURCE}/ rsync://127.0.0.1:$STUNNEL_LISTEN_PORT/data
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

	w.logger.Info("Running first rsync pass (file preservation)", "args", strings.Join(rsyncArgs, " "))

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.logger.Info("First rsync pass failed", "exitCode", exitErr.ExitCode())
			return exitErr.ExitCode(), nil
		}
		w.logger.Error(err, "Failed to execute first rsync pass")
		return 1, err
	}

	w.logger.Info("First rsync pass completed successfully")
	return 0, nil
}

// syncForDeletion performs the second rsync pass to delete extra files on destination
func (w *Worker) syncForDeletion(rsyncTarget string) int {
	// Second pass: delete files that exist on destination but not on source
	// rsync -rx --exclude=lost+found --ignore-existing --ignore-non-existing --delete --itemize-changes --info=stats2,misc2 ${SOURCE}/ rsync://127.0.0.1:$STUNNEL_LISTEN_PORT/data
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

	w.logger.Info("Running second rsync pass (deletion)", "args", strings.Join(rsyncArgs, " "))

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			w.logger.Info("Second rsync pass failed", "exitCode", exitErr.ExitCode())
			return exitErr.ExitCode()
		}
		w.logger.Error(err, "Failed to execute second rsync pass")
		return 1
	}

	w.logger.Info("Second rsync pass completed successfully")
	return 0
}

func getFSName(fsa *ca.FSAdmin, locationID int64) (string, error) {
	volumes, err := fsa.EnumerateVolumes()
	if err != nil {
		return "", err
	}
	for _, val := range volumes {
		if val.ID == locationID {
			return val.Name, nil
		}
	}
	return "", ceph.ErrKeyNotFound
}

// processSnapDiffResults reads from the result channel and processes entries until
// the channel is closed or an error occurs.
func processSnapDiffResults(ctx context.Context, resultChan chan snapDiffResult, relPath, dataVolumePath string) error {
	for {
		select {
		case result, ok := <-resultChan:
			if !ok {
				// Channel closed, processing complete
				return nil
			}
			if result.Err != nil {
				return result.Err
			}
			if err := processEntry(result.Entry, relPath, dataVolumePath); err != nil {
				return err
			}

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// processEntry replaces relPath with dataVolumePath in the entry path and rsyncs the entry.
// If the entry does not exist on source, it will be deleted from the destination.
func processEntry(entry *cephfs.SnapDiffEntry, relPath, dataVolumePath string) error {
	// Get the entry path
	entryPath := entry.DirEntry.Name()

	// Replace relPath with dataVolumePath to get the transformed path
	// The entry path starts with relPath, so we replace it with dataVolumePath
	var transformedPath string
	if strings.HasPrefix(entryPath, relPath) {
		transformedPath = strings.Replace(entryPath, relPath, dataVolumePath, 1)
	} else {
		transformedPath = path.Join(dataVolumePath, entryPath)
	}

	// Source path is the transformed path (file on the local filesystem)
	sourcePath := transformedPath

	// Check if the source path exists
	if _, err := os.Stat(sourcePath); err != nil {
		if os.IsNotExist(err) {
			// File or directory does not exist on source, delete from destination
			return deleteFromDestination(transformedPath)
		}
		// Some other error occurred (permission denied, etc.)
		return fmt.Errorf("failed to stat entry %s: %w", sourcePath, err)
	}

	// Build rsync target with the transformed path
	// The destination path should also have relPath replaced with dataVolumePath
	rsyncDaemonPort := os.Getenv("RSYNC_DAEMON_PORT")
	rsyncTarget := fmt.Sprintf("rsync://127.0.0.1:%s%s", rsyncDaemonPort, transformedPath)

	// Rsync the specific entry
	rsyncArgs := []string{
		"-aAhHSxz",
		"--itemize-changes",
		"--info=stats2,misc2",
		sourcePath,
		rsyncTarget,
	}

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to rsync entry %s: %w", sourcePath, err)
	}

	return nil
}

// deleteFromDestination deletes a file or directory from the destination using rsync.
// It uses rsync's --delete flag with include/exclude patterns to target the specific entry.
func deleteFromDestination(transformedPath string) error {
	rsyncDaemonPort := os.Getenv("RSYNC_DAEMON_PORT")
	parentDir := path.Dir(transformedPath)
	fileName := path.Base(transformedPath)

	rsyncTarget := fmt.Sprintf("rsync://127.0.0.1:%s%s/", rsyncDaemonPort, parentDir)

	// Use rsync to delete the specific file/directory from destination
	// --include specifies the file to delete, --exclude=* ignores everything else
	// --delete will remove the file from destination since it doesn't exist in source
	rsyncArgs := []string{
		"-rx",
		"--delete",
		fmt.Sprintf("--include=%s", fileName),
		"--exclude=*",
		"--itemize-changes",
		"--info=stats2,misc2",
		parentDir + "/",
		rsyncTarget,
	}

	log.Printf("Deleting entry %s from destination", transformedPath)

	cmd := exec.Command("rsync", rsyncArgs...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to delete entry %s from destination: %w", transformedPath, err)
	}

	return nil
}

func initSnapDiffChan(mountInfo *cephfs.MountInfo, dataVolumePath, rootPath, relPath, baseSnapName, targetSnapName string) chan snapDiffResult {
	resultChan := make(chan snapDiffResult, 100)

	go func() {
		defer close(resultChan)

		dirEntryList := []*cephfs.SnapDiffEntry{
			{},
		}
		newDirEntryList := []*cephfs.SnapDiffEntry{}
		for len(dirEntryList) > 0 {
			for i := range len(dirEntryList) {
				currentEntry := dirEntryList[i]
				currentRelPath := ""
				if currentEntry.DirEntry != nil {
					currentRelPath = currentEntry.DirEntry.Name()
				} else {
					currentRelPath = relPath
				}
				diffConfig := cephfs.SnapDiffConfig{
					CMount:   mountInfo,
					RootPath: rootPath,
					RelPath:  currentRelPath,
					Snap1:    baseSnapName,
					Snap2:    targetSnapName,
				}
				log.Default().Printf("Opening snap diff for rootPath=%s, relPath=%s, snap1=%s, snap2=%s\n",
					diffConfig.RootPath, diffConfig.RelPath, diffConfig.Snap1, diffConfig.Snap2)

				diffInfo, err := cephfs.OpenSnapDiff(diffConfig)
				if err != nil {
					resultChan <- snapDiffResult{Err: fmt.Errorf("failed to open snap diff: %w", err)}
					return
				}
				defer diffInfo.Close()

				for {
					entry, err := diffInfo.Readdir()
					if err != nil {
						resultChan <- snapDiffResult{Err: fmt.Errorf("failed to read snap diff entry: %w", err)}
						return
					}
					if entry == nil {
						break
					}
					if entry.DirEntry.DType() == cephfs.DTypeDir {
						name := entry.DirEntry.Name()
						if !(name == "." || name == "..") {
							// append only if it is not current or previous directory.
							newDirEntryList = append(newDirEntryList, entry)
						}
						continue
					}
					resultChan <- snapDiffResult{Entry: entry}
				}
				if currentRelPath != relPath {
					resultChan <- snapDiffResult{Entry: currentEntry}
				}
			}
			dirEntryList = newDirEntryList
			newDirEntryList = []*cephfs.SnapDiffEntry{}
		}
	}()

	return resultChan
}
