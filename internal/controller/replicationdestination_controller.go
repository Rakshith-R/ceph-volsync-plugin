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

package controller

import (
	"context"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	sm "github.com/backube/volsync/controllers/statemachine"
	"github.com/backube/volsync/controllers/utils"
	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"

	cephPluginMover "github.com/RamenDR/ceph-volsync-plugin/internal/mover"
)

// ReplicationDestinationReconciler reconciles a ReplicationDestination object
type ReplicationDestinationReconciler struct {
	client.Client
	Log           logr.Logger
	Scheme        *runtime.Scheme
	EventRecorder record.EventRecorder
}

type rdMachine struct {
	rd     *volsyncv1alpha1.ReplicationDestination
	client client.Client
	logger logr.Logger
	// TODO: uncomment when metrics are implemented.
	// metrics volsyncMetrics
	mover mover.Mover
}

var _ sm.ReplicationMachine = &rdMachine{}

// +kubebuilder:rbac:groups=volsync.backube,resources=replicationdestinations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationdestinations/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationdestinations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=apps,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;update;patch
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=pods/log,verbs=get;list
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete;escalate;bind
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete;escalate;bind
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch;create;update;patch;delete;escalate;bind
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,resourceNames=volsync-privileged-mover,verbs=use
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=get;list;watch;create;patch;update;use
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotcontents,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch

func (r *ReplicationDestinationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("replicationdestination", req.NamespacedName)
	// Get CR instance
	instance := &volsyncv1alpha1.ReplicationDestination{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if !kerrors.IsNotFound(err) {
			logger.Error(err, "Failed to get Destination")
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if rdHasMover(instance) {
		logger.Info("ReplicationDestination already has a mover job associated, skipping reconciliation")

		return ctrl.Result{}, nil
	}

	if instance.Status == nil {
		instance.Status = &volsyncv1alpha1.ReplicationDestinationStatus{}
	}

	var result ctrl.Result
	var err error

	// Check if any volume snapshots are marked with do-not-delete label and remove ownership if so
	err = utils.RelinquishOwnedSnapshotsWithDoNotDeleteLabel(ctx, r.Client, logger, instance)
	if err != nil {
		return result, err
	}

	rdm, err := newRDMachine(instance, r.Client, logger,
		record.NewEventRecorderAdapter(mover.NewEventRecorderLogger(r.EventRecorder)))

	// No method found
	if rdm == nil {
		logger.Info("No mover found for ReplicationDestination, skipping reconciliation")

		return ctrl.Result{}, nil
	}

	// All good, so run the state machine
	if err == nil {
		result, err = sm.Run(ctx, rdm, logger)
	}

	// Update instance status
	statusErr := r.Client.Status().Update(ctx, instance)
	if err == nil { // Don't mask previous error
		err = statusErr
	}
	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *ReplicationDestinationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&volsyncv1alpha1.ReplicationDestination{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: 100,
		}).
		Owns(&batchv1.Job{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Owns(&corev1.Secret{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&snapv1.VolumeSnapshot{}).
		Complete(r)
}

// rdHasMover checks if the ReplicationDestination has a mover job defined.
func rdHasMover(rd *volsyncv1alpha1.ReplicationDestination) bool {
	return rd.Spec.Rclone != nil ||
		rd.Spec.Restic != nil ||
		rd.Spec.Rsync != nil ||
		rd.Spec.RsyncTLS != nil
}

func newRDMachine(rd *volsyncv1alpha1.ReplicationDestination, c client.Client,
	l logr.Logger, er events.EventRecorder) (*rdMachine, error) {
	dataMover, err := cephPluginMover.GetDestinationMoverFromCatalog(c, l, er, rd, true)
	if err != nil {
		return nil, err
	}

	// TODO: uncomment after metrics are implemented.
	// metrics := newVolSyncMetrics(prometheus.Labels{
	// 	"obj_name":      rd.Name,
	// 	"obj_namespace": rd.Namespace,
	// 	"role":          "destination",
	// 	"method":        dataMover.Name(),
	// })

	return &rdMachine{
		rd:     rd,
		client: c,
		logger: l,
		// TODO: uncomment after metrics are implemented.
		// metrics: metrics,
		mover: dataMover,
	}, nil
}

func (m *rdMachine) Cronspec() string {
	if m.rd.Spec.Trigger != nil && m.rd.Spec.Trigger.Schedule != nil {
		return *m.rd.Spec.Trigger.Schedule
	}
	return ""
}

func (m *rdMachine) ManualTag() string {
	if m.rd.Spec.Trigger != nil {
		return m.rd.Spec.Trigger.Manual
	}
	return ""
}

func (m *rdMachine) LastManualTag() string {
	return m.rd.Status.LastManualSync
}

func (m *rdMachine) SetLastManualTag(tag string) {
	m.rd.Status.LastManualSync = tag
}

func (m *rdMachine) NextSyncTime() *metav1.Time {
	return m.rd.Status.NextSyncTime
}

func (m *rdMachine) SetNextSyncTime(next *metav1.Time) {
	m.rd.Status.NextSyncTime = next
}

func (m *rdMachine) LastSyncStartTime() *metav1.Time {
	return m.rd.Status.LastSyncStartTime
}

func (m *rdMachine) SetLastSyncStartTime(last *metav1.Time) {
	m.rd.Status.LastSyncStartTime = last
}

func (m *rdMachine) LastSyncTime() *metav1.Time {
	return m.rd.Status.LastSyncTime
}

func (m *rdMachine) SetLastSyncTime(last *metav1.Time) {
	m.rd.Status.LastSyncTime = last
}

func (m *rdMachine) LastSyncDuration() *metav1.Duration {
	return m.rd.Status.LastSyncDuration
}

func (m *rdMachine) SetLastSyncDuration(duration *metav1.Duration) {
	m.rd.Status.LastSyncDuration = duration
}

func (m *rdMachine) Conditions() *[]metav1.Condition {
	return &m.rd.Status.Conditions
}

func (m *rdMachine) SetOutOfSync(isOutOfSync bool) {
	// TODO: uncomment after metrics are implemented.
	// if isOutOfSync {
	// 	m.metrics.OutOfSync.Set(1)
	// } else {
	// 	m.metrics.OutOfSync.Set(0)
	// }
}

func (m *rdMachine) IncMissedIntervals() {
	// TODO: uncomment after metrics are implemented.
	// m.metrics.MissedIntervals.Inc()
}

func (m *rdMachine) ObserveSyncDuration(duration time.Duration) {
	// TODO: uncomment after metrics are implemented.
	// m.metrics.SyncDurations.Observe(duration.Seconds())
}

func (m *rdMachine) Synchronize(ctx context.Context) (mover.Result, error) {
	result, err := m.mover.Synchronize(ctx)

	if result.Completed && result.Image != nil {
		// Mark previous latestImage for cleanup if it was a snapshot
		err = utils.MarkOldSnapshotForCleanup(ctx, m.client, m.logger, m.rd,
			m.rd.Status.LatestImage, result.Image)
		if err != nil {
			return mover.InProgress(), err
		}

		m.rd.Status.LatestImage = result.Image
	}

	return result, err
}

func (m *rdMachine) Cleanup(ctx context.Context) (mover.Result, error) {
	return m.mover.Cleanup(ctx)
}
