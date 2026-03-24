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
	"fmt"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	// k8sClient is a controller-runtime client for typed
	// access to VolSync CRDs and VolumeSnapshots.
	k8sClient client.Client

	// k8sClientSet is a client-go Clientset for typed
	// access to core Kubernetes resources like PVCs.
	k8sClientSet *kubernetes.Clientset
)

// TestE2E runs the end-to-end (e2e) test suite for the project. These tests execute in an isolated,
// temporary environment to validate project changes with the purposed to be used in CI jobs.
// The default setup requires Minikube, builds/loads the Manager Docker image locally, and installs
// CertManager.
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting ceph-volsync-plugin-operator integration test suite\n")
	suiteConfig, reporterConfig := GinkgoConfiguration()
	suiteConfig.FailFast = true
	RunSpecs(t, "e2e suite", suiteConfig, reporterConfig)
}

var _ = BeforeSuite(func() {
	By("setting up controller-runtime client")
	scheme := runtime.NewScheme()
	Expect(clientgoscheme.AddToScheme(scheme)).To(Succeed())
	Expect(volsyncv1alpha1.AddToScheme(scheme)).To(Succeed())
	Expect(snapv1.AddToScheme(scheme)).To(Succeed())

	cfg := ctrl.GetConfigOrDie()
	var err error
	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme})
	Expect(err).NotTo(HaveOccurred())
	Expect(k8sClient).NotTo(BeNil())

	k8sClientSet, err = kubernetes.NewForConfig(cfg)
	Expect(err).NotTo(HaveOccurred())
})
