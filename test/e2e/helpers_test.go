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

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RamenDR/ceph-volsync-plugin/test/utils"
)

// syncTimeout is the timeout for sync operations (env: SYNC_TIMEOUT, default 5m).
var syncTimeout = getEnvDuration("SYNC_TIMEOUT", 5*time.Minute)

// waitTimeout is the timeout for resource waits like PVC bind, pod completion (env: WAIT_TIMEOUT, default 2m).
var waitTimeout = getEnvDuration("WAIT_TIMEOUT", 2*time.Minute)

// dataMatchPollInterval is how often we retry the full validate flow when polling for data match.
const dataMatchPollInterval = 30 * time.Second

// innerPollInterval is the poll interval for sub-waits inside tryValidateSyncedData.
const innerPollInterval = 5 * time.Second

func getEnvDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil {
			return d
		}
		fmt.Fprintf(os.Stderr, "WARNING: invalid %s=%q, using default %s\n", key, v, def)
	}
	return def
}

// driverConfig holds driver-specific parameters for parameterized e2e tests.
type driverConfig struct {
	name                string
	provider            string
	storageClass        string
	volumeSnapshotClass string
	volumeMode          *corev1.PersistentVolumeMode
	accessMode          corev1.PersistentVolumeAccessMode
}

// drivers is the list of storage drivers to test.
var drivers = []driverConfig{
	{
		// TODO: uncomment after nfs support shallow volume
		// 	name: "nfs",
		// 	provider:            "rook-ceph.nfs.csi.ceph.com",
		// 	storageClass:        "rook-nfs",
		// 	volumeSnapshotClass: "csi-nfsplugin-snapclass",
		// }, {
		name:                "cephfs",
		provider:            "rook-ceph.cephfs.csi.ceph.com",
		storageClass:        "rook-cephfs",
		volumeSnapshotClass: "csi-cephfsplugin-snapclass",
	}, {
		name:                "rbd",
		provider:            "rook-ceph.rbd.csi.ceph.com",
		storageClass:        "rook-ceph-block",
		volumeSnapshotClass: "csi-rbdplugin-snapclass",
		volumeMode:          ptr.To(corev1.PersistentVolumeBlock),
		accessMode:          corev1.ReadWriteOnce,
	},
}

// createAndWaitForPVC creates a 1Gi PVC with the given StorageClass and waits for it to be bound.
// It respects driver-specific volumeMode and accessMode settings, defaulting to Filesystem mode
// and ReadWriteMany access.
func createAndWaitForPVC(ctx context.Context, name string, drv driverConfig) {
	By("creating PVC " + name)

	accessMode := corev1.ReadWriteMany
	if drv.accessMode != "" {
		accessMode = drv.accessMode
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{accessMode},
			StorageClassName: ptr.To(drv.storageClass),
			VolumeMode:       drv.volumeMode,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	_, err := k8sClientSet.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())

	Eventually(func(g Gomega) {
		got, err := k8sClientSet.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got.Status.Phase).To(Equal(corev1.ClaimBound))
	}).WithTimeout(waitTimeout).Should(Succeed())
}

// createRDAndWaitForAddress creates a ReplicationDestination and waits for its address and
// keySecret to be published. Returns both.
func createRDAndWaitForAddress(
	ctx context.Context,
	name, destPVC string,
	trigger *volsyncv1alpha1.ReplicationDestinationTriggerSpec,
	drv driverConfig,
	extraParams map[string]string,
) (string, string) {
	By("creating ReplicationDestination " + name)

	params := map[string]string{
		"destinationPVC":          destPVC,
		"storageClassName":        drv.storageClass,
		"volumeSnapshotClassName": drv.volumeSnapshotClass,
	}
	for k, v := range extraParams {
		params[k] = v
	}

	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: trigger,
			External: &volsyncv1alpha1.ReplicationDestinationExternalSpec{
				Provider:   drv.provider,
				Parameters: params,
			},
		},
	}
	Expect(k8sClient.Create(ctx, rd)).To(Succeed())

	var addr, key string

	Eventually(func(g Gomega) {
		got := &volsyncv1alpha1.ReplicationDestination{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, got)).To(Succeed())
		g.Expect(got.Status).NotTo(BeNil())
		g.Expect(got.Status.RsyncTLS).NotTo(BeNil())
		g.Expect(got.Status.RsyncTLS.Address).NotTo(BeNil())
		g.Expect(*got.Status.RsyncTLS.Address).NotTo(BeEmpty())
		g.Expect(got.Status.RsyncTLS.KeySecret).NotTo(BeNil())
		g.Expect(*got.Status.RsyncTLS.KeySecret).NotTo(BeEmpty())

		addr = *got.Status.RsyncTLS.Address
		key = *got.Status.RsyncTLS.KeySecret
	}).WithTimeout(waitTimeout).Should(Succeed())

	return addr, key
}

// createRS creates a ReplicationSource with the given driver config and extra parameters.
func createRS(
	ctx context.Context,
	name, srcPVC string,
	trigger *volsyncv1alpha1.ReplicationSourceTriggerSpec,
	rdAddr, rdKey string,
	drv driverConfig,
	extraParams map[string]string,
) {
	By("creating ReplicationSource " + name)

	params := map[string]string{
		"storageClassName":        drv.storageClass,
		"volumeSnapshotClassName": drv.volumeSnapshotClass,
	}
	if rdKey != "" {
		params["keySecret"] = rdKey
	}
	if rdAddr != "" {
		params["address"] = rdAddr
	}
	for k, v := range extraParams {
		params[k] = v
	}

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: volsyncv1alpha1.ReplicationSourceSpec{
			SourcePVC: srcPVC,
			Trigger:   trigger,
			External: &volsyncv1alpha1.ReplicationSourceExternalSpec{
				Provider:   drv.provider,
				Parameters: params,
			},
		},
	}
	Expect(k8sClient.Create(ctx, rs)).To(Succeed())
}

// waitForManualSync waits for RS.Status.LastManualSync to equal manualID.
func waitForManualSync(ctx context.Context, rsName, manualID string) {
	By("waiting for RS sync " + manualID)

	Eventually(func(g Gomega) {
		rs := &volsyncv1alpha1.ReplicationSource{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs)).To(Succeed())
		g.Expect(rs.Status).NotTo(BeNil())
		g.Expect(rs.Status.LastManualSync).To(Equal(manualID))
	}).WithTimeout(syncTimeout).Should(Succeed())
}

// waitForRDManualSync waits for RD.Status.LastManualSync to equal manualID.
func waitForRDManualSync(ctx context.Context, rdName, manualID string) {
	By("waiting for RD sync " + manualID)

	Eventually(func(g Gomega) {
		rd := &volsyncv1alpha1.ReplicationDestination{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rdName, Namespace: namespace}, rd)).To(Succeed())
		g.Expect(rd.Status).NotTo(BeNil())
		g.Expect(rd.Status.LastManualSync).To(Equal(manualID))
	}).WithTimeout(syncTimeout).Should(Succeed())
}

// createVolumeSnapshot creates a VolumeSnapshot from the given PVC and waits for ReadyToUse.
func createVolumeSnapshot(ctx context.Context, name, pvcName, volumeSnapshotClass string) {
	By("creating VolumeSnapshot " + name)

	snap := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: snapv1.VolumeSnapshotSpec{
			VolumeSnapshotClassName: ptr.To(volumeSnapshotClass),
			Source: snapv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: ptr.To(pvcName),
			},
		},
	}
	Expect(k8sClient.Create(ctx, snap)).To(Succeed())

	Eventually(func(g Gomega) {
		got := &snapv1.VolumeSnapshot{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, got)).To(Succeed())
		g.Expect(got.Status).NotTo(BeNil())
		g.Expect(got.Status.ReadyToUse).NotTo(BeNil())
		g.Expect(*got.Status.ReadyToUse).To(BeTrue())
	}).WithTimeout(syncTimeout).Should(Succeed())
}

// updateManualTrigger patches both RS and RD with a new manual trigger value.
func updateManualTrigger(ctx context.Context, rsName, rdName, newID string) {
	By("updating manual trigger to " + newID)

	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rs := &volsyncv1alpha1.ReplicationSource{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs); err != nil {
			return err
		}
		rs.Spec.Trigger.Manual = newID
		return k8sClient.Update(ctx, rs)
	})).To(Succeed())

	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rd := &volsyncv1alpha1.ReplicationDestination{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rdName, Namespace: namespace}, rd); err != nil {
			return err
		}
		rd.Spec.Trigger.Manual = newID
		return k8sClient.Update(ctx, rd)
	})).To(Succeed())
}

func updateRSManualTrigger(ctx context.Context, rsName, newID string) {
	By("updating RS manual trigger to " + newID)

	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rs := &volsyncv1alpha1.ReplicationSource{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs); err != nil {
			return err
		}
		rs.Spec.Trigger.Manual = newID
		return k8sClient.Update(ctx, rs)
	})).To(Succeed())
}

func setRSPaused(ctx context.Context, rsName string, paused bool) {
	action := "pausing"
	if !paused {
		action = "unpausing"
	}

	By(action + " ReplicationSource " + rsName)

	Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
		rs := &volsyncv1alpha1.ReplicationSource{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs); err != nil {
			return err
		}
		rs.Spec.Paused = paused
		return k8sClient.Update(ctx, rs)
	})).To(Succeed())
}

// waitForSyncTime waits for RS.Status.LastSyncTime to be non-nil.
func waitForSyncTime(ctx context.Context, rsName string) {
	By("waiting for first sync time")

	Eventually(func(g Gomega) {
		rs := &volsyncv1alpha1.ReplicationSource{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rsName, Namespace: namespace}, rs)).To(Succeed())
		g.Expect(rs.Status).NotTo(BeNil())
		g.Expect(rs.Status.LastSyncTime).NotTo(BeNil())
	}).WithTimeout(syncTimeout).Should(Succeed())
}

// waitForRDSyncTime waits for RD.Status.LastSyncTime to be non-nil.
func waitForRDSyncTime(ctx context.Context, rdName string) {
	By("waiting for RD first sync time")

	Eventually(func(g Gomega) {
		rd := &volsyncv1alpha1.ReplicationDestination{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rdName, Namespace: namespace}, rd)).To(Succeed())
		g.Expect(rd.Status).NotTo(BeNil())
		g.Expect(rd.Status.LastSyncTime).NotTo(BeNil())
	}).WithTimeout(syncTimeout).Should(Succeed())
}

// waitForRDNextSync waits for RD.Status.LastSyncTime to be strictly after prevTime.
func waitForRDNextSync(ctx context.Context, rdName string, prevTime *metav1.Time) {
	By("waiting for RD next sync after " + prevTime.String())

	Eventually(func(g Gomega) {
		rd := &volsyncv1alpha1.ReplicationDestination{}
		g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rdName, Namespace: namespace}, rd)).To(Succeed())
		g.Expect(rd.Status).NotTo(BeNil())
		g.Expect(rd.Status.LastSyncTime).NotTo(BeNil())
		g.Expect(rd.Status.LastSyncTime.Time.After(prevTime.Time)).To(BeTrue())
	}).WithTimeout(syncTimeout).WithPolling(15 * time.Second).Should(Succeed())
}

// cleanupReplication deletes RS, RD, and PVCs.
func cleanupReplication(ctx context.Context, rsName, rdName string, pvcNames []string) {
	By("cleaning up replication resources")

	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: namespace,
		},
	}
	_ = client.IgnoreNotFound(k8sClient.Delete(ctx, rs))

	rd := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rdName,
			Namespace: namespace,
		},
	}
	_ = client.IgnoreNotFound(k8sClient.Delete(ctx, rd))

	for _, name := range pvcNames {
		_ = k8sClientSet.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	}
}

// cleanupSnapshots deletes the named VolumeSnapshots.
func cleanupSnapshots(ctx context.Context, snapNames []string) {
	for _, name := range snapNames {
		snap := &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}
		_ = client.IgnoreNotFound(k8sClient.Delete(ctx, snap))
	}
}

// debugAfterEach collects controller pod logs, Kubernetes events, and pod description when a
// spec fails.
func debugAfterEach() {
	specReport := CurrentSpecReport()
	if !specReport.Failed() {
		return
	}

	By("Fetching all ReplicationSources")
	cmd := exec.Command("kubectl", "get", "replicationsources", "-n", namespace, "-o", "yaml")
	rsOutput, err := utils.Run(cmd)
	if err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "ReplicationSources:\n%s", rsOutput)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get ReplicationSources: %s\n", err)
	}

	By("Fetching all ReplicationDestinations")
	cmd = exec.Command("kubectl", "get", "replicationdestinations", "-n", namespace, "-o", "yaml")
	rdOutput, err := utils.Run(cmd)
	if err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "ReplicationDestinations:\n%s", rdOutput)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get ReplicationDestinations: %s\n", err)
	}

	By("Fetching volsync mover pod logs")
	cmd = exec.Command(
		"kubectl", "get", "pods",
		"-l", "app.kubernetes.io/part-of=ceph-volsync-plugin",
		"-n", namespace,
		"-o", "jsonpath={.items[*].metadata.name}",
	)
	moverPodNames, err := utils.Run(cmd)
	if err != nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to list mover pods: %s\n", err)
	} else if moverPodNames != "" {
		for _, moverPod := range strings.Fields(moverPodNames) {
			cmd = exec.Command("kubectl", "logs", moverPod, "-n", namespace, "--all-containers", "--previous")
			prevLogs, prevErr := utils.Run(cmd)
			if prevErr == nil && prevLogs != "" {
				_, _ = fmt.Fprintf(GinkgoWriter, "Mover pod %s previous logs:\n%s\n", moverPod, prevLogs)
			}

			cmd = exec.Command("kubectl", "logs", moverPod, "-n", namespace, "--all-containers")
			moverLogs, logErr := utils.Run(cmd)
			if logErr == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Mover pod %s logs:\n%s\n", moverPod, moverLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get logs for mover pod %s: %s\n", moverPod, logErr)
			}
		}
	}

	By("Fetching controller pod name")
	cmd = exec.Command(
		"kubectl", "get", "pods",
		"-l", "control-plane=controller-manager",
		"-n", namespace,
		"-o", "jsonpath={.items[0].metadata.name}",
	)
	podName, err := utils.Run(cmd)
	if err != nil || podName == "" {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get controller pod name: %v\n", err)
		return
	}

	By("Fetching controller manager pod logs")
	cmd = exec.Command("kubectl", "logs", podName, "-n", namespace)
	controllerLogs, err := utils.Run(cmd)
	if err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
	}

	By("Fetching Kubernetes events")
	cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
	eventsOutput, err := utils.Run(cmd)
	if err == nil {
		_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
	} else {
		_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
	}

	By("Fetching controller manager pod description")
	cmd = exec.Command("kubectl", "describe", "pod", podName, "-n", namespace)
	podDescription, err := utils.Run(cmd)
	if err == nil {
		fmt.Println("Pod description:\n", podDescription)
	} else {
		fmt.Println("Failed to describe controller pod")
	}
}

// runPodWithPVC creates a pod that mounts a PVC and runs a shell command, then waits for it to
// complete. The caller is responsible for deleting the pod after use.
func runPodWithPVC(ctx context.Context, podName, pvcName string, drv driverConfig, command string) {
	By("creating pod " + podName + " with PVC " + pvcName)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "worker",
					Image:           "busybox",
					ImagePullPolicy: corev1.PullAlways,
					Command:         []string{"/bin/sh", "-c", command},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
		},
	}

	isBlock := drv.volumeMode != nil && *drv.volumeMode == corev1.PersistentVolumeBlock

	if isBlock {
		pod.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
			{
				Name:       "vol",
				DevicePath: "/dev/block",
			},
		}
	} else {
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
			{
				Name:      "vol",
				MountPath: "/data",
			},
		}
	}

	By("submitting pod " + podName)

	_, err := k8sClientSet.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())

	By("waiting for pod " + podName + " to succeed")

	Eventually(func(g Gomega) {
		got, err := k8sClientSet.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got.Status.Phase).To(Equal(corev1.PodSucceeded))
	}).WithTimeout(waitTimeout).Should(Succeed())
}

// writeDataToPVC writes test data to a PVC using a short-lived pod. The phase argument selects
// which data set to write.
func writeDataToPVC(ctx context.Context, pvcName string, drv driverConfig, phase int) {
	podName := fmt.Sprintf("write-%s-p%d", pvcName, phase)

	isBlock := drv.volumeMode != nil && *drv.volumeMode == corev1.PersistentVolumeBlock

	var command string

	if isBlock {
		switch phase {
		case 1:
			command = "dd if=/dev/urandom of=/dev/block bs=4096 count=64 && sync"
		case 2:
			command = "dd if=/dev/urandom of=/dev/block bs=4096 count=64 seek=64 && sync"
		}
	} else {
		switch phase {
		case 1:
			command = "mkdir -p /data/dir1/subdir" +
				" && echo 'file1-content' > /data/file1.txt" +
				" && echo 'file2-content' > /data/dir1/file2.txt" +
				" && echo 'subdir-content' > /data/dir1/subdir/file3.txt" +
				" && echo 'to-delete' > /data/deleteme.txt" +
				" && mkdir -p /data/removedir" +
				" && echo 'gone' > /data/removedir/gone.txt" +
				" && sync"
		case 2:
			command = "echo 'file1-modified' > /data/file1.txt" +
				" && echo 'new-file' > /data/newfile.txt" +
				" && rm -f /data/deleteme.txt" +
				" && rm -rf /data/removedir" +
				" && sync"
		}
	}

	runPodWithPVC(ctx, podName, pvcName, drv, command)

	By("deleting write pod " + podName)

	_ = k8sClientSet.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
}

// compareDataInPod creates a single pod that mounts both PVCs and runs a direct comparison
// command. For block devices it uses cmp; for filesystems it uses diff -r. The test fails with
// diagnostic output if data does not match.
func compareDataInPod(ctx context.Context, podName, srcPVC, dstPVC string, drv driverConfig) {
	By("creating comparison pod " + podName)

	isBlock := drv.volumeMode != nil && *drv.volumeMode == corev1.PersistentVolumeBlock

	var command string
	if isBlock {
		command = "cmp /dev/src-block /dev/dst-block"
	} else {
		command = "diff -r /src /dst"
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "compare",
					Image:           "busybox",
					ImagePullPolicy: corev1.PullAlways,
					Command:         []string{"/bin/sh", "-c", command},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "src-vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: srcPVC,
							ReadOnly:  true,
						},
					},
				},
				{
					Name: "dst-vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: dstPVC,
							ReadOnly:  true,
						},
					},
				},
			},
		},
	}

	if isBlock {
		pod.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
			{
				Name:       "src-vol",
				DevicePath: "/dev/src-block",
			},
			{
				Name:       "dst-vol",
				DevicePath: "/dev/dst-block",
			},
		}
	} else {
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
			{
				Name:      "src-vol",
				MountPath: "/src",
				ReadOnly:  true,
			},
			{
				Name:      "dst-vol",
				MountPath: "/dst",
				ReadOnly:  true,
			},
		}
	}

	_, err := k8sClientSet.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())

	By("waiting for comparison pod " + podName)

	var phase corev1.PodPhase

	Eventually(func(g Gomega) {
		got, err := k8sClientSet.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		phase = got.Status.Phase
		g.Expect(phase).To(SatisfyAny(Equal(corev1.PodSucceeded), Equal(corev1.PodFailed)))
	}).WithTimeout(waitTimeout).Should(Succeed())

	if phase == corev1.PodFailed {
		cmd := exec.Command("kubectl", "logs", podName, "-n", namespace)
		output, _ := utils.Run(cmd)

		_ = k8sClientSet.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})

		Fail("data mismatch between source and restored destination:\n" + output)
	}

	By("deleting comparison pod " + podName)

	_ = k8sClientSet.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
}

// validateSyncedData verifies that the data on the destination matches the source by restoring
// a snapshot to a temporary PVC and comparing both PVCs directly in a single pod.
func validateSyncedData(
	ctx context.Context,
	srcPVC, destPVC string,
	drv driverConfig,
	copyMethod, rdName, snapPrefix string,
) {
	var snapName string

	if copyMethod == "Snapshot" {
		By("getting snapshot from RD latestImage")

		rd := &volsyncv1alpha1.ReplicationDestination{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: rdName, Namespace: namespace}, rd)).To(Succeed())
		Expect(rd.Status).NotTo(BeNil())
		Expect(rd.Status.LatestImage).NotTo(BeNil())
		snapName = rd.Status.LatestImage.Name
	} else {
		By("creating validation snapshot " + snapPrefix + "-validate")

		snapName = snapPrefix + "-validate"
		snap := &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      snapName,
				Namespace: namespace,
			},
			Spec: snapv1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: ptr.To(drv.volumeSnapshotClass),
				Source: snapv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: ptr.To(destPVC),
				},
			},
		}
		Expect(k8sClient.Create(ctx, snap)).To(Succeed())

		Eventually(func(g Gomega) {
			got := &snapv1.VolumeSnapshot{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: snapName, Namespace: namespace}, got)).To(Succeed())
			g.Expect(got.Status).NotTo(BeNil())
			g.Expect(got.Status.ReadyToUse).NotTo(BeNil())
			g.Expect(*got.Status.ReadyToUse).To(BeTrue())
		}).WithTimeout(syncTimeout).Should(Succeed())
	}

	By("restoring snapshot to temp PVC " + snapPrefix + "-temp")

	tempPVC := snapPrefix + "-temp"
	accessMode := corev1.ReadOnlyMany
	if drv.accessMode != "" {
		accessMode = drv.accessMode
	}

	snapAPIGroup := "snapshot.storage.k8s.io"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tempPVC,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{accessMode},
			StorageClassName: ptr.To(drv.storageClass),
			VolumeMode:       drv.volumeMode,
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: &snapAPIGroup,
				Kind:     "VolumeSnapshot",
				Name:     snapName,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	_, err := k8sClientSet.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred())

	Eventually(func(g Gomega) {
		got, err := k8sClientSet.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, tempPVC, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(got.Status.Phase).To(Equal(corev1.ClaimBound))
	}).WithTimeout(waitTimeout).Should(Succeed())

	By("comparing source and destination data")

	compareDataInPod(ctx, snapPrefix+"-compare", srcPVC, tempPVC, drv)

	By("cleaning up temp PVC " + tempPVC)

	_ = k8sClientSet.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, tempPVC, metav1.DeleteOptions{})

	if copyMethod == "Direct" {
		By("cleaning up validation snapshot")

		snap := &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      snapName,
				Namespace: namespace,
			},
		}
		_ = client.IgnoreNotFound(k8sClient.Delete(ctx, snap))
	}
}

// pollForSnapshotReady polls until the named VolumeSnapshot reports ReadyToUse == true or the
// timeout expires.
func pollForSnapshotReady(ctx context.Context, snapName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		got := &snapv1.VolumeSnapshot{}

		err := k8sClient.Get(ctx, types.NamespacedName{Name: snapName, Namespace: namespace}, got)
		if err == nil && got.Status != nil && got.Status.ReadyToUse != nil && *got.Status.ReadyToUse {
			return nil
		}

		time.Sleep(innerPollInterval)
	}

	return fmt.Errorf("snapshot %s not ready within %v", snapName, timeout)
}

// pollForPVCBound polls until the named PVC reaches the Bound phase or the timeout expires.
func pollForPVCBound(ctx context.Context, pvcName string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		got, err := k8sClientSet.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
		if err == nil && got.Status.Phase == corev1.ClaimBound {
			return nil
		}

		time.Sleep(innerPollInterval)
	}

	return fmt.Errorf("PVC %s not bound within %v", pvcName, timeout)
}

// pollForPodDone polls until the named pod reaches Succeeded or Failed phase or the timeout
// expires.
func pollForPodDone(ctx context.Context, podName string, timeout time.Duration) (corev1.PodPhase, error) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		got, err := k8sClientSet.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
		if err == nil {
			phase := got.Status.Phase
			if phase == corev1.PodSucceeded || phase == corev1.PodFailed {
				return phase, nil
			}
		}

		time.Sleep(innerPollInterval)
	}

	return "", fmt.Errorf("pod %s did not complete within %v", podName, timeout)
}

// cleanupTryResources deletes temp resources left over from a previous polling iteration. All
// errors are ignored.
func cleanupTryResources(ctx context.Context, snapPrefix, copyMethod string) {
	deleteAndWaitFor(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapPrefix + "-compare",
			Namespace: namespace,
		},
	})

	deleteAndWaitFor(ctx, &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapPrefix + "-temp",
			Namespace: namespace,
		},
	})

	if copyMethod == "Direct" {
		deleteAndWaitFor(ctx, &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      snapPrefix + "-validate",
				Namespace: namespace,
			},
		})
	}
}

func deleteAndWaitFor(ctx context.Context, obj client.Object) {
	_ = client.IgnoreNotFound(k8sClient.Delete(ctx, obj))
	key := types.NamespacedName{
		Name:      obj.GetName(),
		Namespace: obj.GetNamespace(),
	}
	Eventually(func() bool {
		err := k8sClient.Get(ctx, key, obj)
		return apierrors.IsNotFound(err)
	}).WithPolling(innerPollInterval).WithTimeout(waitTimeout).Should(BeTrue())
}

// tryCompareDataInPod creates a comparison pod and returns an error if data does not match,
// instead of calling Fail().
func tryCompareDataInPod(ctx context.Context, podName, srcPVC, dstPVC string, drv driverConfig) error {
	isBlock := drv.volumeMode != nil && *drv.volumeMode == corev1.PersistentVolumeBlock

	var command string
	if isBlock {
		command = "cmp /dev/src-block /dev/dst-block"
	} else {
		command = "diff -r /src /dst"
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:            "compare",
					Image:           "busybox",
					ImagePullPolicy: corev1.PullAlways,
					Command:         []string{"/bin/sh", "-c", command},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "src-vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: srcPVC,
							ReadOnly:  true,
						},
					},
				},
				{
					Name: "dst-vol",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: dstPVC,
							ReadOnly:  true,
						},
					},
				},
			},
		},
	}

	if isBlock {
		pod.Spec.Containers[0].VolumeDevices = []corev1.VolumeDevice{
			{
				Name:       "src-vol",
				DevicePath: "/dev/src-block",
			},
			{
				Name:       "dst-vol",
				DevicePath: "/dev/dst-block",
			},
		}
	} else {
		pod.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
			{
				Name:      "src-vol",
				MountPath: "/src",
				ReadOnly:  true,
			},
			{
				Name:      "dst-vol",
				MountPath: "/dst",
				ReadOnly:  true,
			},
		}
	}

	_, err := k8sClientSet.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating compare pod: %w", err)
	}

	phase, err := pollForPodDone(ctx, podName, waitTimeout)
	if err != nil {
		return err
	}

	if phase == corev1.PodFailed {
		cmd := exec.Command("kubectl", "logs", podName, "-n", namespace)
		output, _ := utils.Run(cmd)

		_ = k8sClientSet.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})

		return fmt.Errorf("data mismatch:\n%s", output)
	}

	_ = k8sClientSet.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{})

	return nil
}

// tryValidateSyncedData performs the full validate flow (snapshot restore + comparison) but
// returns an error instead of calling Expect/Fail. This allows it to be used inside Eventually
// for polling.
func tryValidateSyncedData(
	ctx context.Context,
	srcPVC, destPVC string,
	drv driverConfig,
	copyMethod, rdName, snapPrefix string,
) error {
	cleanupTryResources(ctx, snapPrefix, copyMethod)

	var snapName string

	if copyMethod == "Snapshot" {
		rd := &volsyncv1alpha1.ReplicationDestination{}

		err := k8sClient.Get(ctx, types.NamespacedName{Name: rdName, Namespace: namespace}, rd)
		if err != nil {
			return fmt.Errorf("getting RD: %w", err)
		}

		if rd.Status == nil || rd.Status.LatestImage == nil {
			return fmt.Errorf("RD %s has no latestImage", rdName)
		}

		snapName = rd.Status.LatestImage.Name
	} else {
		snapName = snapPrefix + "-validate"
		snap := &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name:      snapName,
				Namespace: namespace,
			},
			Spec: snapv1.VolumeSnapshotSpec{
				VolumeSnapshotClassName: ptr.To(drv.volumeSnapshotClass),
				Source: snapv1.VolumeSnapshotSource{
					PersistentVolumeClaimName: ptr.To(destPVC),
				},
			},
		}

		if err := k8sClient.Create(ctx, snap); err != nil {
			return fmt.Errorf("creating snapshot: %w", err)
		}

		if err := pollForSnapshotReady(ctx, snapName, syncTimeout); err != nil {
			return err
		}
	}

	tempPVC := snapPrefix + "-temp"
	accessMode := corev1.ReadOnlyMany

	if drv.accessMode != "" {
		accessMode = drv.accessMode
	}

	snapAPIGroup := "snapshot.storage.k8s.io"
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tempPVC,
			Namespace: namespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{accessMode},
			StorageClassName: ptr.To(drv.storageClass),
			VolumeMode:       drv.volumeMode,
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: &snapAPIGroup,
				Kind:     "VolumeSnapshot",
				Name:     snapName,
			},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse("1Gi"),
				},
			},
		},
	}

	_, err := k8sClientSet.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating temp PVC: %w", err)
	}

	if err := pollForPVCBound(ctx, tempPVC, waitTimeout); err != nil {
		return err
	}

	if err := tryCompareDataInPod(ctx, snapPrefix+"-compare", srcPVC, tempPVC, drv); err != nil {
		return err
	}

	cleanupTryResources(ctx, snapPrefix, copyMethod)

	return nil
}

// waitForDataMatch polls every 30 seconds until the deadline, running the full validate flow
// each iteration. Replaces the separate "should validate" step with integrated polling.
func waitForDataMatch(
	ctx context.Context,
	srcPVC, destPVC string,
	drv driverConfig,
	copyMethod, rdName, snapPrefix string,
	deadline *metav1.Time,
) {
	By("polling for data match every 30s")

	timeout := time.Until(deadline.Time)
	if timeout <= 0 {
		timeout = dataMatchPollInterval
	}

	Eventually(func() error {
		return tryValidateSyncedData(ctx, srcPVC, destPVC, drv, copyMethod, rdName, snapPrefix)
	}).WithPolling(dataMatchPollInterval).WithTimeout(timeout).Should(Succeed())
}
