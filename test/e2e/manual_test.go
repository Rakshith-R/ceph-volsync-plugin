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

package e2e

import (
	"context"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe(
	"Manual Trigger Replication",
	func() {
		for _, drv := range drivers {
			drv := drv

			manualDirectTest(drv)
			manualSnapshotTest(drv)
		}
	},
)

func manualDirectTest(drv driverConfig) {
	Context(drv.name+" Direct", Ordered,
		func() {
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
				cleanupReplication(
					ctx, rsName, rdName,
					[]string{
						srcPVC, destPVC,
					},
				)
				cleanupSnapshots(
					ctx,
					[]string{
						baseSnap,
						targetSnap,
					},
				)
			})

			AfterEach(debugAfterEach)

			It("should create PVCs", func() {
				createAndWaitForPVC(
					ctx, srcPVC, drv.sc,
				)
				createAndWaitForPVC(
					ctx, destPVC, drv.sc,
				)
			})

			It("should create RD", func() {
				rdAddr, rdKey =
					createRDAndWaitForAddress(
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

			// TODO: write data to source PVC

			It("should sync just source PVC",
				func() {
					createRS(
						ctx, rsName, srcPVC,
						&volsyncv1alpha1.ReplicationSourceTriggerSpec{
							Manual: manualID1,
						},
						rdAddr, rdKey,
						drv,
						map[string]string{
							"copyMethod": "Direct",
						},
					)
					waitForManualSync(
						ctx, rsName,
						manualID1,
						5*time.Minute,
					)
				},
			)

			It("should take base snapshot",
				func() {
					createVolumeSnapshot(
						ctx,
						baseSnap,
						srcPVC,
						drv.vsClass,
					)
				},
			)

			// TODO: write data to source PVC

			It("should take target snapshot",
				func() {
					createVolumeSnapshot(
						ctx,
						targetSnap,
						srcPVC,
						drv.vsClass,
					)
				},
			)

			It("should sync with base+target"+
				" snap",
				func() {
					rs := &volsyncv1alpha1.
						ReplicationSource{}
					Expect(k8sClient.Get(
						ctx,
						types.NamespacedName{
							Name:      rsName,
							Namespace: namespace,
						},
						rs,
					)).To(Succeed())
					rs.Spec.Trigger.Manual =
						manualID2
					rs.Spec.External.Parameters["volumeName"] = srcPVC
					rs.Spec.External.Parameters["baseSnapshotName"] = baseSnap
					rs.Spec.External.Parameters["targetSnapshotName"] = targetSnap
					Expect(
						k8sClient.Update(
							ctx, rs,
						),
					).To(Succeed())

					rd := &volsyncv1alpha1.
						ReplicationDestination{}
					Expect(k8sClient.Get(
						ctx,
						types.NamespacedName{
							Name:      rdName,
							Namespace: namespace,
						},
						rd,
					)).To(Succeed())
					rd.Spec.Trigger.Manual =
						manualID2
					Expect(
						k8sClient.Update(
							ctx, rd,
						),
					).To(Succeed())

					waitForManualSync(
						ctx, rsName,
						manualID2,
						5*time.Minute,
					)
				},
			)
		},
	)
}

func manualSnapshotTest(drv driverConfig) {
	Context(drv.name+" Snapshot", Ordered,
		func() {
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
				cleanupReplication(
					ctx, rsName, rdName,
					[]string{
						srcPVC, destPVC,
					},
				)
			})

			AfterEach(debugAfterEach)

			It("should create PVCs", func() {
				createAndWaitForPVC(
					ctx, srcPVC, drv.sc,
				)
				createAndWaitForPVC(
					ctx, destPVC, drv.sc,
				)
			})

			It("should create RD", func() {
				rdAddr, rdKey =
					createRDAndWaitForAddress(
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

			// TODO: write data to source PVC

			It("should first sync "+
				"(just source PVC)",
				func() {
					createRS(
						ctx, rsName, srcPVC,
						&volsyncv1alpha1.ReplicationSourceTriggerSpec{
							Manual: manualID1,
						},
						rdAddr, rdKey,
						drv,
						map[string]string{
							"copyMethod": "Snapshot",
						},
					)
					waitForManualSync(
						ctx, rsName,
						manualID1,
						5*time.Minute,
					)
				},
			)

			// TODO: write data to source PVC

			It("should second sync ",
				func() {
					updateManualTrigger(
						ctx,
						rsName,
						rdName,
						manualID2,
					)
					waitForManualSync(
						ctx, rsName,
						manualID2,
						5*time.Minute,
					)
				},
			)
		},
	)
}
