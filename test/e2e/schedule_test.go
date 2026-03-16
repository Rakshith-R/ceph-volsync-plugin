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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

const schedule = "*/3 * * * *"

var _ = Describe(
	"Schedule Replication",
	func() {
		for _, drv := range drivers {
			drv := drv

			scheduleSnapshotTest(drv)
			scheduleDirectTest(drv)
		}
	},
)

func scheduleSnapshotTest(drv driverConfig) {
	Context(drv.name+" Snapshot", Ordered,
		func() {
			srcPVC := drv.name + "-ss-src"
			destPVC := drv.name + "-ss-dest"
			rdName := drv.name + "-ss-rd"
			rsName := drv.name + "-ss-rs"

			var (
				rdAddr              string
				rdKey               string
				lastSyncBeforeWrite *metav1.Time
			)

			ctx := context.TODO()

			AfterAll(func() {
				cleanupReplication(
					ctx, rsName, rdName,
					[]string{
						srcPVC, destPVC,
						drv.name + "-ss-v1-temp",
						drv.name + "-ss-v2-temp",
					},
				)
			})

			AfterEach(debugAfterEach)

			It("should create PVCs", func() {
				createAndWaitForPVC(
					ctx, srcPVC, drv,
				)
				createAndWaitForPVC(
					ctx, destPVC, drv,
				)
			})

			It("should create RD", func() {
				rdAddr, rdKey =
					createRDAndWaitForAddress(
						ctx, rdName, destPVC,
						&volsyncv1alpha1.ReplicationDestinationTriggerSpec{
							Schedule: ptr.To(
								schedule,
							),
						},
						drv,
						map[string]string{
							"copyMethod": "Snapshot",
						},
					)
			})

			It("should write initial data",
				func() {
					writeDataToPVC(
						ctx, srcPVC, drv, 1,
					)
				},
			)

			It("should create RS", func() {
				createRS(
					ctx, rsName, srcPVC,
					&volsyncv1alpha1.ReplicationSourceTriggerSpec{
						Schedule: ptr.To(
							schedule,
						),
					},
					rdAddr, rdKey,
					drv,
					map[string]string{
						"copyMethod": "Snapshot",
					},
				)
			})

			It("should complete first sync",
				func() {
					waitForSyncTime(
						ctx, rsName,
						5*time.Minute,
					)
					waitForRDSyncTime(
						ctx, rdName,
						5*time.Minute,
					)
				},
			)

			It("should validate first sync",
				func() {
					validateSyncedData(
						ctx, srcPVC,
						destPVC, drv,
						"Snapshot", rdName,
						drv.name+"-ss-v1",
					)
				},
			)

			It("should write more data",
				func() {
					lastSyncBeforeWrite =
						waitForSyncTime(
							ctx, rsName,
							5*time.Minute,
						)
					writeDataToPVC(
						ctx, srcPVC, drv, 2,
					)
				},
			)

			It("should complete second sync",
				func() {
					// Wait for 2 RS sync cycles:
					// the first may have raced
					// with the data write.
					rsT := waitForNextSync(
						ctx, rsName,
						lastSyncBeforeWrite,
						10*time.Minute,
					)
					rsT2 := waitForNextSync(
						ctx, rsName, rsT,
						10*time.Minute,
					)
					// Wait for RD to sync after
					// the 2nd RS sync, ensuring
					// RD has the new data.
					waitForRDNextSync(
						ctx, rdName, rsT2,
						10*time.Minute,
					)
				},
			)

			It("should validate second sync",
				func() {
					validateSyncedData(
						ctx, srcPVC,
						destPVC, drv,
						"Snapshot", rdName,
						drv.name+"-ss-v2",
					)
				},
			)
		},
	)
}

func scheduleDirectTest(drv driverConfig) {
	Context(drv.name+" Direct", Ordered,
		func() {
			srcPVC := drv.name + "-sd-src"
			destPVC := drv.name + "-sd-dest"
			rdName := drv.name + "-sd-rd"
			rsName := drv.name + "-sd-rs"

			var (
				rdAddr              string
				rdKey               string
				lastSyncBeforeWrite *metav1.Time
			)

			ctx := context.TODO()

			AfterAll(func() {
				cleanupReplication(
					ctx, rsName, rdName,
					[]string{
						srcPVC, destPVC,
						drv.name + "-sd-v1-temp",
						drv.name + "-sd-v2-temp",
					},
				)
				cleanupSnapshots(
					ctx, []string{
						drv.name + "-sd-v1-validate",
						drv.name + "-sd-v2-validate",
					},
				)
			})

			AfterEach(debugAfterEach)

			It("should create PVCs", func() {
				createAndWaitForPVC(
					ctx, srcPVC, drv,
				)
				createAndWaitForPVC(
					ctx, destPVC, drv,
				)
			})

			It("should create RD", func() {
				rdAddr, rdKey =
					createRDAndWaitForAddress(
						ctx, rdName, destPVC,
						&volsyncv1alpha1.ReplicationDestinationTriggerSpec{
							Schedule: ptr.To(
								schedule,
							),
						},
						drv,
						map[string]string{
							"copyMethod": "Direct",
						},
					)
			})

			It("should write initial data",
				func() {
					writeDataToPVC(
						ctx, srcPVC, drv, 1,
					)
				},
			)

			It("should create RS", func() {
				createRS(
					ctx, rsName, srcPVC,
					&volsyncv1alpha1.ReplicationSourceTriggerSpec{
						Schedule: ptr.To(
							schedule,
						),
					},
					rdAddr, rdKey,
					drv,
					map[string]string{
						"copyMethod": "Snapshot",
					},
				)
			})

			It("should complete first sync",
				func() {
					waitForSyncTime(
						ctx, rsName,
						5*time.Minute,
					)
					waitForRDSyncTime(
						ctx, rdName,
						5*time.Minute,
					)
				},
			)

			It("should validate first sync",
				func() {
					validateSyncedData(
						ctx, srcPVC,
						destPVC, drv,
						"Direct", rdName,
						drv.name+"-sd-v1",
					)
				},
			)

			It("should write more data",
				func() {
					lastSyncBeforeWrite =
						waitForSyncTime(
							ctx, rsName,
							5*time.Minute,
						)
					writeDataToPVC(
						ctx, srcPVC, drv, 2,
					)
				},
			)

			It("should complete second sync",
				func() {
					// Wait for 2 RS sync cycles:
					// the first may have raced
					// with the data write.
					rsT := waitForNextSync(
						ctx, rsName,
						lastSyncBeforeWrite,
						10*time.Minute,
					)
					rsT2 := waitForNextSync(
						ctx, rsName, rsT,
						10*time.Minute,
					)
					// Wait for RD to sync after
					// the 2nd RS sync, ensuring
					// RD has the new data.
					waitForRDNextSync(
						ctx, rdName, rsT2,
						10*time.Minute,
					)
				},
			)

			It("should validate second sync",
				func() {
					validateSyncedData(
						ctx, srcPVC,
						destPVC, drv,
						"Direct", rdName,
						drv.name+"-sd-v2",
					)
				},
			)
		},
	)
}
