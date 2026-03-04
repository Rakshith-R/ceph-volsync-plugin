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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/RamenDR/ceph-volsync-plugin/test/utils"
)

// namespace where the project is deployed in
const namespace = "rook-ceph"

// serviceAccountName created for the project
const serviceAccountName = "ceph-volsync-plugin-operator-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "ceph-volsync-plugin-operator-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "ceph-volsync-plugin-operator-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		// TODO: metrics tests are currently skipped
		// uncomment when metrics tests are enabled
		// By("cleaning up the curl pod for metrics")
		// cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		// _, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
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

			// TODO: metrics tests are currently skipped
			// uncomment when metrics tests are enabled
			// By("Fetching curl-metrics logs")
			// cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			// metricsOutput, err := utils.Run(cmd)
			// if err == nil {
			// 	_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			// } else {
			// 	_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			// }

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				// Get the name of the controller-manager pod
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				// Validate the pod's status
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			// TODO: metrics tests are currently skipped
			// remove Skip() once metrics are functional
			Skip("Skipping metrics test")
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=ceph-volsync-plugin-operator-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("waiting for the metrics endpoint to be ready")
			verifyMetricsEndpointReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "endpoints", metricsServiceName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("8443"), "Metrics endpoint is not ready")
			}
			Eventually(verifyMetricsEndpointReady).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("controller-runtime.metrics\tServing metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted).Should(Succeed())

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": ["curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics"],
							"securityContext": {
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccount": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			metricsOutput := getMetricsOutput()
			Expect(metricsOutput).To(ContainSubstring(
				"controller_runtime_reconcile_total",
			))
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks
	})

	Context("CephFS Replication", Ordered, func() {
		const (
			cephfsProvider = "rook-ceph." +
				"cephfs.csi.ceph.com"
			scName   = "rook-cephfs"
			vsClass  = "csi-cephfsplugin-snapclass"
			srcPVC   = "cephfs-e2e-src-pvc"
			destPVC  = "cephfs-e2e-dest-pvc"
			rdName   = "cephfs-e2e-dest"
			rsName   = "cephfs-e2e-src"
			manualID = "e2e-1"
		)

		var (
			rdAddress   string
			rdKeySecret string
		)

		f := framework.NewDefaultFramework("cephfs")
		f.SkipNamespaceCreation = true

		ctx := context.TODO()

		AfterAll(func() {
			By("cleaning up replication resources")
			rs := &volsyncv1alpha1.ReplicationSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      rsName,
					Namespace: namespace,
				},
			}
			_ = client.IgnoreNotFound(
				k8sClient.Delete(ctx, rs),
			)

			rd := &volsyncv1alpha1.ReplicationDestination{
				ObjectMeta: metav1.ObjectMeta{
					Name:      rdName,
					Namespace: namespace,
				},
			}
			_ = client.IgnoreNotFound(
				k8sClient.Delete(ctx, rd),
			)

			for _, name := range []string{
				srcPVC, destPVC,
			} {
				_ = f.ClientSet.CoreV1().
					PersistentVolumeClaims(namespace).
					Delete(
						ctx, name,
						metav1.DeleteOptions{},
					)
			}
		})

		It("should create source PVC", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      srcPVC,
					Namespace: namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteMany,
					},
					StorageClassName: ptr.To(scName),
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}
			_, err := f.ClientSet.CoreV1().
				PersistentVolumeClaims(namespace).
				Create(
					ctx, pvc, metav1.CreateOptions{},
				)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				got, err := f.ClientSet.CoreV1().
					PersistentVolumeClaims(namespace).
					Get(
						ctx, srcPVC,
						metav1.GetOptions{},
					)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got.Status.Phase).To(
					Equal(corev1.ClaimBound),
				)
			}).WithTimeout(
				2 * time.Minute,
			).Should(Succeed())
		})

		It("should create destination PVC", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      destPVC,
					Namespace: namespace,
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteMany,
					},
					StorageClassName: ptr.To(scName),
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			}
			_, err := f.ClientSet.CoreV1().
				PersistentVolumeClaims(namespace).
				Create(
					ctx, pvc, metav1.CreateOptions{},
				)
			Expect(err).NotTo(HaveOccurred())

			Eventually(func(g Gomega) {
				got, err := f.ClientSet.CoreV1().
					PersistentVolumeClaims(namespace).
					Get(
						ctx, destPVC,
						metav1.GetOptions{},
					)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got.Status.Phase).To(
					Equal(corev1.ClaimBound),
				)
			}).WithTimeout(
				2 * time.Minute,
			).Should(Succeed())
		})

		It("should create RD and publish address",
			func() {
				rd := &volsyncv1alpha1.ReplicationDestination{
					ObjectMeta: metav1.ObjectMeta{
						Name:      rdName,
						Namespace: namespace,
					},
					Spec: volsyncv1alpha1.ReplicationDestinationSpec{
						Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{
							Manual: manualID,
						},
						External: &volsyncv1alpha1.ReplicationDestinationExternalSpec{
							Provider: cephfsProvider,
							Parameters: map[string]string{
								"destinationPVC":          destPVC,
								"storageClassName":        scName,
								"volumeSnapshotClassName": vsClass,
								"copyMethod":              "Snapshot",
							},
						},
					},
				}
				Expect(
					k8sClient.Create(ctx, rd),
				).To(Succeed())

				Eventually(func(g Gomega) {
					got := &volsyncv1alpha1.ReplicationDestination{}
					g.Expect(k8sClient.Get(
						ctx,
						types.NamespacedName{
							Name:      rdName,
							Namespace: namespace,
						},
						got,
					)).To(Succeed())
					g.Expect(got.Status).NotTo(BeNil())
					g.Expect(
						got.Status.RsyncTLS,
					).NotTo(BeNil())
					g.Expect(
						got.Status.RsyncTLS.Address,
					).NotTo(BeNil())
					g.Expect(
						*got.Status.RsyncTLS.Address,
					).NotTo(BeEmpty())
					g.Expect(
						got.Status.RsyncTLS.KeySecret,
					).NotTo(BeNil())
					g.Expect(
						*got.Status.RsyncTLS.KeySecret,
					).NotTo(BeEmpty())

					rdAddress =
						*got.Status.RsyncTLS.Address
					rdKeySecret =
						*got.Status.RsyncTLS.KeySecret
				}).WithTimeout(
					5 * time.Minute,
				).Should(Succeed())
			},
		)

		It("should create ReplicationSource", func() {
			rs := &volsyncv1alpha1.ReplicationSource{
				ObjectMeta: metav1.ObjectMeta{
					Name:      rsName,
					Namespace: namespace,
				},
				Spec: volsyncv1alpha1.ReplicationSourceSpec{
					SourcePVC: srcPVC,
					Trigger: &volsyncv1alpha1.ReplicationSourceTriggerSpec{
						Manual: manualID,
					},
					External: &volsyncv1alpha1.ReplicationSourceExternalSpec{
						Provider: cephfsProvider,
						Parameters: map[string]string{
							"secretKey":               rdKeySecret,
							"address":                 rdAddress,
							"storageClassName":        scName,
							"volumeSnapshotClassName": vsClass,
							"copyMethod":              "Snapshot",
						},
					},
				},
			}
			Expect(
				k8sClient.Create(ctx, rs),
			).To(Succeed())
		})

		It("should create a VolumeSnapshot", func() {
			labelKey := "volsync.backube/" +
				"snapshot-status-" + rsName
			Eventually(func(g Gomega) {
				snapList := &snapv1.VolumeSnapshotList{}
				g.Expect(k8sClient.List(
					ctx, snapList,
					client.InNamespace(namespace),
					client.MatchingLabels{
						labelKey: "current",
					},
				)).To(Succeed())
				g.Expect(
					len(snapList.Items),
				).To(BeNumerically(">=", 1))
				snap := &snapList.Items[0]
				g.Expect(snap.Status).NotTo(BeNil())
				g.Expect(
					snap.Status.ReadyToUse,
				).NotTo(BeNil())
				g.Expect(
					*snap.Status.ReadyToUse,
				).To(BeTrue())
			}).WithTimeout(
				5 * time.Minute,
			).Should(Succeed())
		})

		It("should complete sync", func() {
			Eventually(func(g Gomega) {
				rs := &volsyncv1alpha1.ReplicationSource{}
				g.Expect(k8sClient.Get(
					ctx,
					types.NamespacedName{
						Name:      rsName,
						Namespace: namespace,
					},
					rs,
				)).To(Succeed())
				g.Expect(rs.Status).NotTo(BeNil())
				g.Expect(
					rs.Status.LastManualSync,
				).To(Equal(manualID))
			}).WithTimeout(
				5 * time.Minute,
			).Should(Succeed())
		})
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	// Temporary file to store the token request
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		// Execute kubectl command to create the token
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		// Parse the JSON output to extract the token
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() string {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	metricsOutput, err := utils.Run(cmd)
	Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
	Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
	return metricsOutput
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
