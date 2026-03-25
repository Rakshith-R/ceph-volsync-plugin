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
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("Manual Trigger Replication", func() {
	for _, drv := range drivers {
		manualDirectTest(drv)
		manualSnapshotTest(drv)
		manualDirectNoRDTriggerTest(drv)
		manualSnapshotNoRDTriggerTest(drv)
	}
})

func manualDirectTest(drv driverConfig) {
	Context(drv.name+" Direct", Ordered, func() {
		srcPVC := drv.name + "-md-src"
		destPVC := drv.name + "-md-dest"
		rdName := drv.name + "-md-rd"
		rsName := drv.name + "-md-rs"
		manualID1 := drv.name + "-md-1"
		manualID2 := drv.name + "-md-2"
		baseSnap := drv.name + "-md-base"
		targetSnap := drv.name + "-md-target"

		var (
			rdAddr string
			rdKey  string
		)

		ctx := context.TODO()

		AfterAll(func() {
			cleanupReplication(ctx, rsName, rdName, []string{
				srcPVC, destPVC,
				drv.name + "-md-v1-temp",
				drv.name + "-md-v2-temp",
			})
			cleanupSnapshots(ctx, []string{
				baseSnap,
				targetSnap,
				drv.name + "-md-v1-validate",
				drv.name + "-md-v2-validate",
			})
		})

		AfterEach(debugAfterEach)

		It("should create PVCs", func() {
			createAndWaitForPVC(ctx, srcPVC, drv)
			createAndWaitForPVC(ctx, destPVC, drv)
		})

		It("should create RD", func() {
			rdAddr, rdKey = createRDAndWaitForAddress(
				ctx, rdName, destPVC,
				&volsyncv1alpha1.ReplicationDestinationTriggerSpec{
					Manual: manualID1,
				},
				drv,
				map[string]string{
					"copyMethod": "Direct",
				},
			)
		})

		It("should write initial data", func() {
			writeDataToPVC(ctx, srcPVC, drv, 1)
		})

		It("should sync just source PVC", func() {
			createRS(
				ctx, rsName, srcPVC,
				&volsyncv1alpha1.ReplicationSourceTriggerSpec{
					Manual: manualID1,
				},
				rdAddr, rdKey, drv,
				map[string]string{
					"copyMethod": "Direct",
				},
			)
			waitForManualSync(ctx, rsName, manualID1)
			waitForRDManualSync(ctx, rdName, manualID1)
		})

		It("should validate first sync", func() {
			validateSyncedData(ctx, srcPVC, destPVC, drv, "Direct", rdName, drv.name+"-md-v1")
		})

		It("should take base snapshot", func() {
			createVolumeSnapshot(ctx, baseSnap, srcPVC, drv.volumeSnapshotClass)
		})

		It("should write more data", func() {
			writeDataToPVC(ctx, srcPVC, drv, 2)
		})

		It("should take target snapshot", func() {
			createVolumeSnapshot(ctx, targetSnap, srcPVC, drv.volumeSnapshotClass)
		})

		It("should sync with base+target snap", func() {
			rs := &volsyncv1alpha1.ReplicationSource{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      rsName,
				Namespace: namespace,
			}, rs)).To(Succeed())
			rs.Spec.Trigger.Manual = manualID2
			rs.Spec.External.Parameters["volumeName"] = srcPVC
			rs.Spec.External.Parameters["baseSnapshotName"] = baseSnap
			rs.Spec.External.Parameters["targetSnapshotName"] = targetSnap
			Expect(k8sClient.Update(ctx, rs)).To(Succeed())

			rd := &volsyncv1alpha1.ReplicationDestination{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      rdName,
				Namespace: namespace,
			}, rd)).To(Succeed())
			rd.Spec.Trigger.Manual = manualID2
			Expect(k8sClient.Update(ctx, rd)).To(Succeed())

			waitForManualSync(ctx, rsName, manualID2)
			waitForRDManualSync(ctx, rdName, manualID2)
		})

		It("should validate second sync", func() {
			validateSyncedData(ctx, srcPVC, destPVC, drv, "Direct", rdName, drv.name+"-md-v2")
		})
	})
}

func manualSnapshotTest(drv driverConfig) {
	Context(drv.name+" Snapshot", Ordered, func() {
		srcPVC := drv.name + "-ms-src"
		destPVC := drv.name + "-ms-dest"
		rdName := drv.name + "-ms-rd"
		rsName := drv.name + "-ms-rs"
		manualID1 := drv.name + "-ms-1"
		manualID2 := drv.name + "-ms-2"

		var (
			rdAddr string
			rdKey  string
		)

		ctx := context.TODO()

		AfterAll(func() {
			cleanupReplication(ctx, rsName, rdName, []string{
				srcPVC, destPVC,
				drv.name + "-ms-v1-temp",
				drv.name + "-ms-v2-temp",
			})
		})

		AfterEach(debugAfterEach)

		It("should create PVCs", func() {
			createAndWaitForPVC(ctx, srcPVC, drv)
			createAndWaitForPVC(ctx, destPVC, drv)
		})

		It("should create RD", func() {
			rdAddr, rdKey = createRDAndWaitForAddress(
				ctx, rdName, destPVC,
				&volsyncv1alpha1.ReplicationDestinationTriggerSpec{
					Manual: manualID1,
				},
				drv,
				map[string]string{
					"copyMethod": "Snapshot",
				},
			)
		})

		It("should write initial data", func() {
			writeDataToPVC(ctx, srcPVC, drv, 1)
		})

		It("should first sync (just source PVC)", func() {
			createRS(
				ctx, rsName, srcPVC,
				&volsyncv1alpha1.ReplicationSourceTriggerSpec{
					Manual: manualID1,
				},
				rdAddr, rdKey, drv,
				map[string]string{
					"copyMethod": "Snapshot",
				},
			)
			waitForManualSync(ctx, rsName, manualID1)
			waitForRDManualSync(ctx, rdName, manualID1)
		})

		It("should validate first sync", func() {
			validateSyncedData(ctx, srcPVC, destPVC, drv, "Snapshot", rdName, drv.name+"-ms-v1")
		})

		It("should write more data", func() {
			writeDataToPVC(ctx, srcPVC, drv, 2)
		})

		It("should second sync", func() {
			updateManualTrigger(ctx, rsName, rdName, manualID2)
			waitForManualSync(ctx, rsName, manualID2)
			waitForRDManualSync(ctx, rdName, manualID2)
		})

		It("should validate second sync", func() {
			validateSyncedData(ctx, srcPVC, destPVC, drv, "Snapshot", rdName, drv.name+"-ms-v2")
		})
	})
}

func manualDirectNoRDTriggerTest(drv driverConfig) {
	Context(drv.name+" Direct No RD Trigger", Ordered, func() {
		srcPVC := drv.name + "-mdn-src"
		destPVC := drv.name + "-mdn-dest"
		rdName := drv.name + "-mdn-rd"
		rsName := drv.name + "-mdn-rs"
		manualID1 := drv.name + "-mdn-1"
		manualID2 := drv.name + "-mdn-2"
		baseSnap := drv.name + "-mdn-base"
		targetSnap := drv.name + "-mdn-target"

		var (
			rdAddr    string
			rdKey     string
			firstSync *metav1.Time
		)

		ctx := context.TODO()

		AfterAll(func() {
			cleanupReplication(ctx, rsName, rdName, []string{
				srcPVC, destPVC,
				drv.name + "-mdn-v1-temp",
				drv.name + "-mdn-v2-temp",
			})
			cleanupSnapshots(ctx, []string{
				baseSnap,
				targetSnap,
				drv.name + "-mdn-v1-validate",
				drv.name + "-mdn-v2-validate",
			})
		})

		AfterEach(debugAfterEach)

		It("should create PVCs", func() {
			createAndWaitForPVC(ctx, srcPVC, drv)
			createAndWaitForPVC(ctx, destPVC, drv)
		})

		It("should create RD", func() {
			rdAddr, rdKey = createRDAndWaitForAddress(
				ctx, rdName, destPVC, nil, drv,
				map[string]string{
					"copyMethod": "Direct",
				},
			)
		})

		It("should write initial data", func() {
			writeDataToPVC(ctx, srcPVC, drv, 1)
		})

		It("should sync just source PVC", func() {
			createRS(
				ctx, rsName, srcPVC,
				&volsyncv1alpha1.ReplicationSourceTriggerSpec{
					Manual: manualID1,
				},
				rdAddr, rdKey, drv,
				map[string]string{
					"copyMethod": "Direct",
				},
			)
			waitForManualSync(ctx, rsName, manualID1)
			waitForRDSyncTime(ctx, rdName)
		})

		It("should validate first sync", func() {
			validateSyncedData(ctx, srcPVC, destPVC, drv, "Direct", rdName, drv.name+"-mdn-v1")
		})

		It("should take base snapshot", func() {
			createVolumeSnapshot(ctx, baseSnap, srcPVC, drv.volumeSnapshotClass)
		})

		It("should write more data", func() {
			writeDataToPVC(ctx, srcPVC, drv, 2)
		})

		It("should take target snapshot", func() {
			createVolumeSnapshot(ctx, targetSnap, srcPVC, drv.volumeSnapshotClass)
		})

		It("should sync with base+target snap", func() {
			firstSync = &metav1.Time{
				Time: time.Now(),
			}

			rs := &volsyncv1alpha1.ReplicationSource{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      rsName,
				Namespace: namespace,
			}, rs)).To(Succeed())
			rs.Spec.Trigger.Manual = manualID2
			rs.Spec.External.Parameters["volumeName"] = srcPVC
			rs.Spec.External.Parameters["baseSnapshotName"] = baseSnap
			rs.Spec.External.Parameters["targetSnapshotName"] = targetSnap
			Expect(k8sClient.Update(ctx, rs)).To(Succeed())

			waitForManualSync(ctx, rsName, manualID2)
			waitForRDNextSync(ctx, rdName, firstSync)
		})

		It("should validate second sync", func() {
			validateSyncedData(ctx, srcPVC, destPVC, drv, "Direct", rdName, drv.name+"-mdn-v2")
		})
	})
}

func manualSnapshotNoRDTriggerTest(drv driverConfig) {
	Context(drv.name+" Snapshot No RD Trigger", Ordered, func() {
		srcPVC := drv.name + "-msn-src"
		destPVC := drv.name + "-msn-dest"
		rdName := drv.name + "-msn-rd"
		rsName := drv.name + "-msn-rs"
		manualID1 := drv.name + "-msn-1"
		manualID2 := drv.name + "-msn-2"

		var (
			rdAddr    string
			rdKey     string
			firstSync *metav1.Time
		)

		ctx := context.TODO()

		AfterAll(func() {
			cleanupReplication(ctx, rsName, rdName, []string{
				srcPVC, destPVC,
				drv.name + "-msn-v1-temp",
				drv.name + "-msn-v2-temp",
			})
		})

		AfterEach(debugAfterEach)

		It("should create PVCs", func() {
			createAndWaitForPVC(ctx, srcPVC, drv)
			createAndWaitForPVC(ctx, destPVC, drv)
		})

		It("should create RD", func() {
			rdAddr, rdKey = createRDAndWaitForAddress(
				ctx, rdName, destPVC, nil, drv,
				map[string]string{
					"copyMethod": "Snapshot",
				},
			)
		})

		It("should write initial data", func() {
			writeDataToPVC(ctx, srcPVC, drv, 1)
		})

		It("should first sync (just source PVC)", func() {
			createRS(
				ctx, rsName, srcPVC,
				&volsyncv1alpha1.ReplicationSourceTriggerSpec{
					Manual: manualID1,
				},
				rdAddr, rdKey, drv,
				map[string]string{
					"copyMethod": "Snapshot",
				},
			)
			waitForManualSync(ctx, rsName, manualID1)
			waitForRDSyncTime(ctx, rdName)
		})

		It("should validate first sync", func() {
			validateSyncedData(ctx, srcPVC, destPVC, drv, "Snapshot", rdName, drv.name+"-msn-v1")
		})

		It("should write more data", func() {
			writeDataToPVC(ctx, srcPVC, drv, 2)
		})

		It("should second sync", func() {
			firstSync = &metav1.Time{
				Time: time.Now(),
			}
			updateRSManualTrigger(ctx, rsName, manualID2)
			waitForManualSync(ctx, rsName, manualID2)
			waitForRDNextSync(ctx, rdName, firstSync)
		})

		It("should validate second sync", func() {
			validateSyncedData(ctx, srcPVC, destPVC, drv, "Snapshot", rdName, drv.name+"-msn-v2")
		})
	})
}
