/*
Copyright 2016 The Kubernetes Authors.

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

package resourcequota

import (
	"encoding/json"

	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/golang/glog"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pkg_runtime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientv1 "k8s.io/client-go/pkg/api/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	fed "k8s.io/kubernetes/federation/apis/federation"
	fedv1 "k8s.io/kubernetes/federation/apis/federation/v1beta1"
	fedclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_clientset"
	fedutil "k8s.io/kubernetes/federation/pkg/federation-controller/util"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/deletionhelper"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util/eventsink"
	//"k8s.io/kubernetes/federation/pkg/federation-controller/util/quotapreferences"
	"k8s.io/kubernetes/pkg/api"
	apiv1 "k8s.io/kubernetes/pkg/api/v1"
	extensionsv1 "k8s.io/kubernetes/pkg/apis/extensions/v1beta1"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	"k8s.io/kubernetes/pkg/controller"
)

const (
	FedResourceQuotaPreferencesAnnotation = "federation.kubernetes.io/resource-quota-preferences"
	allClustersKey                        = "THE_ALL_CLUSTER_KEY"
	UserAgentName                         = "federation-resourcequota-controller"
	ControllerName                        = "replicasets"
)

var (
	RequiredResources           = []schema.GroupVersionResource{extensionsv1.SchemeGroupVersion.WithResource("replicasets")}
	resourceQuotaReviewDelay    = 10 * time.Second
	clusterAvailableDelay       = 20 * time.Second
	clusterUnavailableDelay     = 60 * time.Second
	allResourceQuotaReviewDelay = 2 * time.Minute
	updateTimeout               = 30 * time.Second
	smallDelay                  = 3 * time.Second
)

func parseFederationResourceQuotaPreference(fedResourceQuota *apiv1.ResourceQuota) (*fed.FederatedResourceQuotaPreferences, error) {
	if fedResourceQuota.Annotations == nil {
		return nil, nil
	}
	fedResourceQuotaPrefString, found := fedResourceQuota.Annotations[FedResourceQuotaPreferencesAnnotation]
	if !found {
		return nil, nil
	}
	var fedResourceQuotaPref fed.FederatedResourceQuotaPreferences
	if err := json.Unmarshal([]byte(fedResourceQuotaPrefString), &fedResourceQuotaPref); err != nil {
		return nil, err
	}
	return &fedResourceQuotaPref, nil
}

type ResourceQuotaController struct {

	// Client to federated api server.
	federatedApiClient fedclientset.Interface

	// Contains resourcequotas present in members of federation.
	resourceQuotaFederatedInformer fedutil.FederatedInformer

	// Contains namespaces present in members of federation.
	namespaceFederatedInformer fedutil.FederatedInformer

	// For triggering single resourcequota reconciliation. This is used when there is an
	// add/update/delete operation on a resourcequota in either federated API server or
	// in some member of the federation.
	resourceQuotaDeliverer *fedutil.DelayingDeliverer

	// For triggering all resourcequotas reconciliation. This is used when
	// a new cluster becomes available.
	clusterDeliverer *fedutil.DelayingDeliverer

	resourceQuotaWorkQueue workqueue.Interface

	// For updating members of federation.
	federatedUpdater fedutil.FederatedUpdater
	// Definitions of resourcequotas that should be federated.
	resourceQuotaStore cache.Store
	// Informer controller for resourcequotas that should be federated.
	resourceQuotaController cache.Controller

	// clusterMonitorPeriod is the period for updating status of cluster
	resourceQuotaMonitorPeriod time.Duration

	// Backoff manager for resourcequotas
	resourceQuotaBackoff *flowcontrol.Backoff

	// For events
	eventRecorder record.EventRecorder

	deletionHelper *deletionhelper.DeletionHelper
}

// NewResourceQuotaController returns a new resourcequota controller
func NewResourceQuotaController(federationClient fedclientset.Interface) *ResourceQuotaController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(eventsink.NewFederatedEventSink(federationClient))
	recorder := broadcaster.NewRecorder(api.Scheme, clientv1.EventSource{Component: UserAgentName})

	frqc := &ResourceQuotaController{
		federatedApiClient:     federationClient,
		resourceQuotaWorkQueue: workqueue.New(),
		resourceQuotaBackoff:   flowcontrol.NewBackOff(5*time.Second, time.Minute),
		eventRecorder:          recorder,
	}

	// Build delivereres for triggering reconciliations.
	frqc.resourceQuotaDeliverer = fedutil.NewDelayingDeliverer()
	frqc.clusterDeliverer = fedutil.NewDelayingDeliverer()

	resourceQuotaFedInformerFactory := func(cluster *fedv1.Cluster, clientset kubeclientset.Interface) (cache.Store, cache.Controller) {
		return cache.NewInformer(
			&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (pkg_runtime.Object, error) {
					return clientset.CoreV1().ResourceQuotas(metav1.NamespaceAll).List(options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return clientset.CoreV1().ResourceQuotas(metav1.NamespaceAll).Watch(options)
				},
			},
			&apiv1.ResourceQuota{},
			controller.NoResyncPeriodFunc(),
			fedutil.NewTriggerOnAllChanges(
				func(obj pkg_runtime.Object) { frqc.deliverResourceQuotaObj(obj, resourceQuotaReviewDelay, false) },
			),
		)
	}

	clusterLifecycle := fedutil.ClusterLifecycleHandlerFuncs{
		ClusterAvailable: func(cluster *fedv1.Cluster) {
			frqc.clusterDeliverer.DeliverAfter(allClustersKey, nil, clusterAvailableDelay)
		},
		ClusterUnavailable: func(cluster *fedv1.Cluster, _ []interface{}) {
			frqc.clusterDeliverer.DeliverAfter(allClustersKey, nil, clusterUnavailableDelay)
		},
	}

	frqc.resourceQuotaStore, frqc.resourceQuotaController = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (pkg_runtime.Object, error) {
				return frqc.federatedApiClient.CoreV1().ResourceQuotas(metav1.NamespaceAll).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				return frqc.federatedApiClient.CoreV1().ResourceQuotas(metav1.NamespaceAll).Watch(options)
			},
		},
		&apiv1.ResourceQuota{},
		controller.NoResyncPeriodFunc(),
		fedutil.NewTriggerOnMetaAndSpecChanges(
			func(obj pkg_runtime.Object) { frqc.deliverResourceQuotaObj(obj, resourceQuotaReviewDelay, false) },
		),
	)

	frqc.resourceQuotaFederatedInformer = fedutil.NewFederatedInformer(federationClient, resourceQuotaFedInformerFactory, &clusterLifecycle)

	// Federated updater along with Create/Update/Delete operations.
	frqc.federatedUpdater = fedutil.NewFederatedUpdater(frqc.resourceQuotaFederatedInformer, "resourcequota", updateTimeout, frqc.eventRecorder,
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			rq := obj.(*apiv1.ResourceQuota)
			_, err := client.CoreV1().ResourceQuotas(rq.Namespace).Create(rq)
			return err
		},
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			rq := obj.(*apiv1.ResourceQuota)
			_, err := client.CoreV1().ResourceQuotas(rq.Namespace).Update(rq)
			return err
		},
		func(client kubeclientset.Interface, obj pkg_runtime.Object) error {
			rq := obj.(*apiv1.ResourceQuota)
			orphanDependents := false
			err := client.CoreV1().ResourceQuotas(rq.Namespace).Delete(rq.Name, &metav1.DeleteOptions{OrphanDependents: &orphanDependents})
			return err
		})

	frqc.deletionHelper = deletionhelper.NewDeletionHelper(
		frqc.updateResourceQuota,
		// objNameFunc
		func(obj pkg_runtime.Object) string {
			rq := obj.(*apiv1.ResourceQuota)
			return fmt.Sprintf("%s/%s", rq.Namespace, rq.Name)
		},
		frqc.resourceQuotaFederatedInformer,
		frqc.federatedUpdater,
	)

	return frqc
}

// Sends the given updated object to apiserver.
// Assumes that the given object is a replicaset.
func (frqc *ResourceQuotaController) updateResourceQuota(obj pkg_runtime.Object) (pkg_runtime.Object, error) {
	resourcequota := obj.(*apiv1.ResourceQuota)
	return frqc.federatedApiClient.CoreV1().ResourceQuotas(resourcequota.Namespace).Update(resourcequota)
}

func (frqc *ResourceQuotaController) Run(workers int, stopChan <-chan struct{}) {

	go frqc.resourceQuotaController.Run(stopChan)
	frqc.resourceQuotaFederatedInformer.Start()

	frqc.resourceQuotaDeliverer.StartWithHandler(func(item *fedutil.DelayingDelivererItem) {
		resourcequota := item.Value.(*resourcequotaItem)
		glog.V(3).Infof("Calling reconcileResourceQuota: %s/%s", resourcequota.namespace, resourcequota.name)
		frqc.resourceQuotaWorkQueue.Add(getResourceQuotaKey(resourcequota.namespace, resourcequota.name))
	})
	frqc.clusterDeliverer.StartWithHandler(func(_ *fedutil.DelayingDelivererItem) {
		glog.V(3).Infof("Calling reconcileResourceQuotaOnClusterChange()")
		frqc.reconcileResourceQuotasOnClusterChange()
	})

	for !frqc.isSynced() {
		glog.V(3).Infof("Wating sync status")
		time.Sleep(5 * time.Millisecond)
	}

	glog.V(3).Infof("Controller cache is synced, starting workers")
	for i := 0; i < workers; i++ {
		glog.V(3).Infof("Starting worker: %d", i)
		go wait.Until(frqc.worker, time.Second, stopChan)
	}

	fedutil.StartBackoffGC(frqc.resourceQuotaBackoff, stopChan)

	<-stopChan
	glog.V(3).Infof("Shutting down ResourceQuotaController")
	frqc.resourceQuotaDeliverer.Stop()
	frqc.clusterDeliverer.Stop()
	frqc.resourceQuotaWorkQueue.ShutDown()
	frqc.resourceQuotaFederatedInformer.Stop()
}

func getResourceQuotaKey(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

func parseResourceQuotaKey(key string) (string, string) {
	parsed := strings.Split(key, "/")
	namespace := parsed[0]
	name := parsed[1]
	return namespace, name
}

func (frqc *ResourceQuotaController) worker() {
	for {
		item, quit := frqc.resourceQuotaWorkQueue.Get()
		if quit {
			return
		}
		key := item.(string)
		status, err := frqc.reconcileResourceQuota(key)
		frqc.resourceQuotaWorkQueue.Done(item)
		if err != nil {
			glog.Errorf("Error syncing cluster controller: %v", err)
			frqc.deliverResourceQuotaByKey(key, 0, true)
		} else {
			switch status {
			case statusAllOk:
				break
			case statusError:
				frqc.deliverResourceQuotaByKey(key, 0, true)
			case statusNeedRecheck:
				frqc.deliverResourceQuotaByKey(key, resourceQuotaReviewDelay, false)
			case statusNotSynced:
				frqc.deliverResourceQuotaByKey(key, clusterAvailableDelay, false)
			default:
				glog.Errorf("Unhandled reconciliation status: %s", status)
				frqc.deliverResourceQuotaByKey(key, resourceQuotaReviewDelay, false)
			}
		}
	}
}

// Internal structure for data in delaying deliverer.
type resourcequotaItem struct {
	namespace string
	name      string
}

func (frqc *ResourceQuotaController) deliverResourceQuotaObj(obj interface{}, delay time.Duration, failed bool) {
	rq := obj.(*apiv1.ResourceQuota)
	frqc.deliverResourceQuota(rq.Namespace, rq.Name, delay, failed)
}

// Adds backoff to delay if this delivery is related to some failure. Resets backoff if there was no failure.
func (frqc *ResourceQuotaController) deliverResourceQuota(namespace string, name string, delay time.Duration, failed bool) {
	key := getResourceQuotaKey(namespace, name)
	frqc.deliverResourceQuotaByKey(key, delay, failed)
}

// Adds backoff to delay if this delivery is related to some failure. Resets backoff if there was no failure.
func (frqc *ResourceQuotaController) deliverResourceQuotaByKey(key string, delay time.Duration, failed bool) {
	if failed {
		frqc.resourceQuotaBackoff.Next(key, time.Now())
		delay = delay + frqc.resourceQuotaBackoff.Get(key)
	} else {
		frqc.resourceQuotaBackoff.Reset(key)
	}
	namespace, name := parseResourceQuotaKey(key)
	frqc.resourceQuotaDeliverer.DeliverAfter(key,
		&resourcequotaItem{namespace: namespace, name: name}, delay)
}

// Check whether all data stores are in sync. False is returned if any of the informer/stores is not yet
// synced with the corresponding api server.
func (resourcequotacontroller *ResourceQuotaController) isSynced() bool {
	if !resourcequotacontroller.resourceQuotaFederatedInformer.ClustersSynced() {
		glog.V(2).Infof("Cluster list not synced")
		glog.Infof("Cluster list not synced")
		return false
	}
	clusters, err := resourcequotacontroller.resourceQuotaFederatedInformer.GetReadyClusters()
	if err != nil {
		glog.Errorf("Failed to get ready clusters: %v", err)
		return false
	}
	if !resourcequotacontroller.resourceQuotaFederatedInformer.GetTargetStore().ClustersSynced(clusters) {
		glog.Infof("Target clusters not synced")
		return false
	}
	return true
}

// The function triggers reconciliation of all federated resourcequotas.
func (frqc *ResourceQuotaController) reconcileResourceQuotasOnClusterChange() {
	if !frqc.isSynced() {
		frqc.clusterDeliverer.DeliverAt(allClustersKey, nil, time.Now().Add(clusterAvailableDelay))
	}
	for _, obj := range frqc.resourceQuotaStore.List() {
		resourceQuota := obj.(*apiv1.ResourceQuota)
		frqc.deliverResourceQuota(resourceQuota.Namespace, resourceQuota.Name, smallDelay, false)
	}
}

type reconciliationStatus string

const (
	statusAllOk       = reconciliationStatus("ALL_OK")
	statusNeedRecheck = reconciliationStatus("RECHECK")
	statusError       = reconciliationStatus("ERROR")
	statusNotSynced   = reconciliationStatus("NOSYNC")
)

func (frqc *ResourceQuotaController) reconcileResourceQuota(key string) (reconciliationStatus, error) {

	if !frqc.isSynced() {
		glog.Infof("Clusters are not synced")
		return statusNotSynced, nil
	}

	glog.Infof("Clusters are synced, starting reconcile resource quota")
	baseResourceQuotaObjFromStore, exist, err := frqc.resourceQuotaStore.GetByKey(key)
	if err != nil {
		return statusError, err
	}

	if !exist {
		// Not federated resource quota, ignoring.
		glog.Infof("resource quota key does not exist. key=%v", key)
		return statusAllOk, nil
	}

	namespace, resourcequotaName := parseResourceQuotaKey(key)
	// Create a copy before modifying the resource quota to prevent race condition with
	// other readers of resource quota from store.
	baseResourceQuotaObj, err := api.Scheme.DeepCopy(baseResourceQuotaObjFromStore)
	if err != nil {
		glog.Errorf("Error in retrieving obj from store: %v", err)
		return statusError, err
	}

	baseResourceQuota, ok := baseResourceQuotaObj.(*apiv1.ResourceQuota)
	if !ok {
		glog.Errorf("Error in retrieving obj from store with key: %v", key)
		frqc.deliverResourceQuotaByKey(key, 0, true)
		return statusError, fmt.Errorf("Error in retrieving obj from store with key: %s", key)
	}
	if baseResourceQuota.DeletionTimestamp != nil {
		if err := frqc.delete(baseResourceQuota); err != nil {
			glog.Errorf("Failed to delete %s: %v", resourcequotaName, err)
			frqc.eventRecorder.Eventf(baseResourceQuota, api.EventTypeWarning, "DeleteFailed",
				"Resource quota delete failed: %v", err)
			return statusError, err
		}
		return statusAllOk, nil
	}

	glog.V(3).Infof("Ensuring delete object from underlying clusters finalizer for resource quota: %s", key)

	// Add the required finalizers before creating a resource quota in
	// underlying clusters.
	// This ensures that the dependent resource quotas are deleted in underlying
	// clusters when the federated resource quota is deleted.
	updatedResourceQuotaObj, err := frqc.deletionHelper.EnsureFinalizers(baseResourceQuota)
	if err != nil {
		glog.Errorf("Failed to ensure delete object from underlying clusters finalizer in namespace %s: %v",
			baseResourceQuota.Name, err)
		frqc.deliverResourceQuota(namespace, resourcequotaName, smallDelay, true)
		return statusError, err
	}
	baseResourceQuota = updatedResourceQuotaObj.(*apiv1.ResourceQuota)

	glog.V(3).Infof("Syncing resource quota %s in underlying clusters", baseResourceQuota.Name)

	clusters, err := frqc.resourceQuotaFederatedInformer.GetReadyClusters()
	if err != nil {
		return statusError, err
	}

	// Sync resource quota objects between local clusters and FCP
	operations := make([]fedutil.FederatedOperation, 0)

	var frqStatus apiv1.ResourceQuotaStatus
	if baseResourceQuota.Status.Hard == nil {
		frqStatus.Hard = baseResourceQuota.Spec.Hard
	} else {
		frqStatus.Hard = baseResourceQuota.Status.Hard
	}

	for _, cluster := range clusters {

		// resource quota object stored in FCP
		frqObj := &apiv1.ResourceQuota{
			ObjectMeta: fedutil.DeepCopyRelevantObjectMeta(baseResourceQuota.ObjectMeta),
			Spec:       baseResourceQuota.Spec,
			Status:     baseResourceQuota.Status,
		}

		// local cluster resource quota object from underlying cluster
		lrqObj, found, err := frqc.resourceQuotaFederatedInformer.GetTargetStore().GetByKey(cluster.Name, key)
		if err != nil {
			return statusError, err
		}

		if !found {
			glog.V(3).Infof("Resource quota object did not found from underlying cluster, creating: %s", key)
			frqc.eventRecorder.Eventf(baseResourceQuota, api.EventTypeNormal, "CreateInCluster",
				"Creating resourcequota in cluster %s", cluster.Name)

			operations = append(operations, fedutil.FederatedOperation{
				Type:        fedutil.OperationTypeAdd,
				Obj:         frqObj,
				ClusterName: cluster.Name,
			})
		} else {
			currentLrq := lrqObj.(*apiv1.ResourceQuota)

			// Summarize local cluster usage data
			frqStatus.Used = add(frqStatus.Used, currentLrq.Status.Used)

			// Update existing resourcequota, if needed.
			if !fedutil.ObjectMetaAndSpecEquivalent(frqObj, currentLrq) {
				glog.V(3).Infof("Resource quota object found from underlying clusters but with different values, updating: %s", key)

				frqc.eventRecorder.Eventf(baseResourceQuota, api.EventTypeNormal, "UpdateInCluster", "Updating resourcequota in cluster %s", cluster.Name)
				operations = append(operations, fedutil.FederatedOperation{
					Type:        fedutil.OperationTypeUpdate,
					Obj:         frqObj,
					ClusterName: cluster.Name,
				})
			}
		}
	}

	if !reflect.DeepEqual(baseResourceQuota.Status, frqStatus) {
		glog.V(3).Infof("Resource quota object found from underlying clusters but with different usage values, updating status for: %s", key)
		baseResourceQuota.Status = frqStatus

		_, err = frqc.federatedApiClient.Core().ResourceQuotas(baseResourceQuota.Namespace).UpdateStatus(baseResourceQuota)
		if err != nil {
			glog.Errorf("failed to update resource quota status %s: %v", resourcequotaName, err)
			return statusError, err
		}
	}

	if len(operations) == 0 {
		// Everything is in order
		return statusAllOk, nil
	}
	err = frqc.federatedUpdater.Update(operations)
	if err != nil {
		glog.Errorf("Failed to execute updates for %s: %v", key, err)
		return statusError, err
	}

	// Some operations were made, reconcile after a while.
	return statusNeedRecheck, nil
}

// Add returns the result of a + b for each named resource
func add(a apiv1.ResourceList, b apiv1.ResourceList) apiv1.ResourceList {
	result := apiv1.ResourceList{}
	for key, value := range a {
		quantity := *value.Copy()
		if other, found := b[key]; found {
			quantity.Add(other)
		}
		result[key] = quantity
	}
	for key, value := range b {
		if _, found := result[key]; !found {
			quantity := *value.Copy()
			result[key] = quantity
		}
	}
	return result
}

// delete  deletes the given resource quota or returns error if the deletion was not complete.
func (frqc *ResourceQuotaController) delete(resourceQuota *apiv1.ResourceQuota) error {
	var err error
	glog.V(3).Infof("Handling deletion of resource quota %s ...", resourceQuota.Name)
	// Delete the resource quota from all underlying clusters.
	_, err = frqc.deletionHelper.HandleObjectInUnderlyingClusters(resourceQuota)
	if err != nil {
		return err
	}

	/*resourcequotacontroller.eventRecorder.Event(resourceQuota, api.EventTypeNormal, "DeleteResourceQuota", fmt.Sprintf("Marking for deletion"))
	err = frqc.federatedApiClient.Core().ResourceQuotas(resourceQuota.Namespace).Delete(resourceQuota.Name, &metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete resource quota: %v", err)
	}*/

	err = frqc.federatedApiClient.CoreV1().ResourceQuotas(resourceQuota.Namespace).Delete(resourceQuota.Name, nil)
	if err != nil {
		// Its all good if the error is not found error. That means it is deleted already and we do not have to do anything.
		// This is expected when we are processing an update as a result of resource quota finalizer deletion.
		// The process that deleted the last finalizer is also going to delete the resource quota and we do not have to do anything.
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to delete resource quota: %v", err)
		}
	}
	return nil
}
