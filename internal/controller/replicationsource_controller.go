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

	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/backube/volsync/controllers/mover"
	sm "github.com/backube/volsync/controllers/statemachine"
	"github.com/backube/volsync/controllers/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	cephPluginMover "github.com/RamenDR/ceph-volsync-plugin/internal/mover"
)

const (
	ReplicationSourceToSourcePVCIndex string = "replicationsource.spec.sourcePVC"
)

// ReplicationSourceReconciler reconciles a ReplicationSource object
type ReplicationSourceReconciler struct {
	client.Client
	Log           logr.Logger
	Scheme        *runtime.Scheme
	EventRecorder record.EventRecorder
}

type rsMachine struct {
	rs     *volsyncv1alpha1.ReplicationSource
	client client.Client
	logger logr.Logger
	mover  mover.Mover
}

// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources/finalizers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=volsync.backube,resources=replicationsources/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;update;patch
// +kubebuilder:rbac:groups=apps,resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=persistentvolumes,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=pods/log,verbs=get;list
// +kubebuilder:rbac:groups=core,resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=namespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;update;patch
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles,verbs=get;list;watch;create;update;patch;delete;escalate;bind
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=rolebindings,verbs=get;list;watch;create;update;patch;delete;escalate;bind
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterrolebindings,verbs=get;list;watch;create;update;patch;delete;escalate;bind
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,resourceNames=volsync-privileged-mover,verbs=use
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=use
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshots,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=snapshot.storage.k8s.io,resources=volumesnapshotcontents,verbs=get;list;watch;create;update;patch;delete;deletecollection
// +kubebuilder:rbac:groups=storage.k8s.io,resources=storageclasses,verbs=get;list;watch
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=get;list;watch;create;patch;update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ReplicationSource object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.18.4/pkg/reconcile
func (r *ReplicationSourceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := r.Log.WithValues("replicationsource", req.NamespacedName)
	instance := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		logger.Error(err, "Failed to fetch ReplicationSource")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	var result ctrl.Result
	var err error

	if rsHasMover(instance) {
		logger.Info("ReplicationSource already has a mover job associated, skipping reconciliation")
		return ctrl.Result{}, nil
	}

	if instance.Status == nil {
		instance.Status = &volsyncv1alpha1.ReplicationSourceStatus{}
	}

	rsm, err := newRSMachine(instance, r.Client, logger,
		record.NewEventRecorderAdapter(mover.NewEventRecorderLogger(r.EventRecorder)))

	if rsm == nil {
		logger.Info("No mover found for ReplicationSource, skipping reconciliation")

		return ctrl.Result{}, nil
	}
	// All good, so run the state machine
	if err == nil {
		result, err = sm.Run(ctx, rsm, logger)
	}

	// Update instance status
	statusErr := r.Client.Status().Update(ctx, instance)
	if err == nil { // Don't mask previous error
		err = statusErr
	}
	return result, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *ReplicationSourceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&volsyncv1alpha1.ReplicationSource{}).
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
		Watches(&corev1.PersistentVolumeClaim{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, o client.Object) []reconcile.Request {
				return mapFuncCopyTriggerPVCToReplicationSource(ctx, mgr.GetClient(), o)
			}), builder.WithPredicates(copyTriggerPVCPredicate())).
		Complete(r)
}

func mapFuncCopyTriggerPVCToReplicationSource(ctx context.Context, k8sClient client.Client,
	o client.Object) []reconcile.Request {
	logger := ctrl.Log.WithName("mapFuncCopyTriggerPVCToReplicationSource")

	pvc, ok := o.(*corev1.PersistentVolumeClaim)
	if !ok {
		return []reconcile.Request{}
	}

	// Only continue if the PVC is using the use-copy-trigger annotation
	if !utils.PVCUsesCopyTrigger(pvc) {
		return []reconcile.Request{}
	}

	// Find if we have any ReplicationSources using this PVC as a source
	// This will break if multiple replicationsources use the same PVC as a source
	rsList := &volsyncv1alpha1.ReplicationSourceList{}
	err := k8sClient.List(ctx, rsList,
		client.MatchingFields{ReplicationSourceToSourcePVCIndex: pvc.GetName()}, // custom index
		client.InNamespace(pvc.GetNamespace()))
	if err != nil {
		logger.Error(err, "Error looking up replicationsources (using index) matching source PVC",
			"pvc name", pvc.GetName(), "namespace", pvc.GetNamespace(),
			"index name", ReplicationSourceToSourcePVCIndex)
		return []reconcile.Request{}
	}

	reqs := []reconcile.Request{}
	for i := range rsList.Items {
		rs := rsList.Items[i]
		reqs = append(reqs, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Name:      rs.GetName(),
				Namespace: rs.GetNamespace(),
			},
		})
	}

	return reqs
}

func copyTriggerPVCPredicate() predicate.Predicate {
	// Only reconcile ReplicationSources for PVC if the PVC is new or updated (no delete)
	return predicate.Funcs{
		CreateFunc: func(_ event.CreateEvent) bool {
			return true
		},
		DeleteFunc: func(_ event.DeleteEvent) bool {
			return false
		},
		UpdateFunc: func(_ event.UpdateEvent) bool {
			return true
		},
		GenericFunc: func(_ event.GenericEvent) bool {
			return true
		},
	}
}

func IndexFieldsForReplicationSource(ctx context.Context, fieldIndexer client.FieldIndexer) error {
	// Index on ReplicationSources - used to find ReplicationSources with SourcePVC referring to a PVC
	return fieldIndexer.IndexField(ctx, &volsyncv1alpha1.ReplicationSource{},
		ReplicationSourceToSourcePVCIndex, func(o client.Object) []string {
			var res []string
			replicationSource, ok := o.(*volsyncv1alpha1.ReplicationSource)
			if !ok {
				// This shouldn't happen
				return res
			}

			// just return the raw field value -- the indexer will take care of dealing with namespaces for us
			sourcePVC := replicationSource.Spec.SourcePVC
			if sourcePVC != "" {
				res = append(res, sourcePVC)
			}
			return res
		})
}

func rsHasMover(rs *volsyncv1alpha1.ReplicationSource) bool {
	return rs.Spec.Rclone != nil ||
		rs.Spec.Restic != nil ||
		rs.Spec.Rsync != nil ||
		rs.Spec.RsyncTLS != nil
}

func newRSMachine(rs *volsyncv1alpha1.ReplicationSource, c client.Client,
	l logr.Logger, er events.EventRecorder) (*rsMachine, error) {
	dataMover, err := cephPluginMover.GetSourceMoverFromCatalog(c, l, er, rs, true)
	if err != nil {
		return nil, err
	}

	return &rsMachine{
		rs:     rs,
		client: c,
		logger: l,
		mover:  dataMover,
	}, nil
}

func (m *rsMachine) Cronspec() string {
	if m.rs.Spec.Trigger != nil && m.rs.Spec.Trigger.Schedule != nil {
		return *m.rs.Spec.Trigger.Schedule
	}
	return ""
}

func (m *rsMachine) ManualTag() string {
	if m.rs.Spec.Trigger != nil {
		return m.rs.Spec.Trigger.Manual
	}
	return ""
}

func (m *rsMachine) LastManualTag() string {
	return m.rs.Status.LastManualSync
}

func (m *rsMachine) SetLastManualTag(tag string) {
	m.rs.Status.LastManualSync = tag
}

func (m *rsMachine) NextSyncTime() *metav1.Time {
	return m.rs.Status.NextSyncTime
}

func (m *rsMachine) SetNextSyncTime(next *metav1.Time) {
	m.rs.Status.NextSyncTime = next
}

func (m *rsMachine) LastSyncStartTime() *metav1.Time {
	return m.rs.Status.LastSyncStartTime
}

func (m *rsMachine) SetLastSyncStartTime(last *metav1.Time) {
	m.rs.Status.LastSyncStartTime = last
}

func (m *rsMachine) LastSyncTime() *metav1.Time {
	return m.rs.Status.LastSyncTime
}

func (m *rsMachine) SetLastSyncTime(last *metav1.Time) {
	m.rs.Status.LastSyncTime = last
}

func (m *rsMachine) LastSyncDuration() *metav1.Duration {
	return m.rs.Status.LastSyncDuration
}

func (m *rsMachine) SetLastSyncDuration(duration *metav1.Duration) {
	m.rs.Status.LastSyncDuration = duration
}

func (m *rsMachine) Conditions() *[]metav1.Condition {
	return &m.rs.Status.Conditions
}

func (m *rsMachine) SetOutOfSync(isOutOfSync bool) {
	// TODO: uncomment once metrics is enabled
	// if isOutOfSync {
	// 	m.metrics.OutOfSync.Set(1)
	// } else {
	// 	m.metrics.OutOfSync.Set(0)
	// }
}

func (m *rsMachine) IncMissedIntervals() {
	// TODO: uncomment once metrics is enabled
	// m.metrics.MissedIntervals.Inc()
}

func (m *rsMachine) ObserveSyncDuration(duration time.Duration) {
	// TODO: uncomment once metrics is enabled
	// m.metrics.SyncDurations.Observe(duration.Seconds())
}

func (m *rsMachine) Synchronize(ctx context.Context) (mover.Result, error) {
	return m.mover.Synchronize(ctx)
}

func (m *rsMachine) Cleanup(ctx context.Context) (mover.Result, error) {
	return m.mover.Cleanup(ctx)
}
