/*
Copyright 2017 The Kubernetes Authors.

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

package federatedtypes

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pkgruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	federationclientset "k8s.io/kubernetes/federation/client/clientset_generated/federation_clientset"
	"k8s.io/kubernetes/federation/pkg/federation-controller/util"
	apiv1 "k8s.io/kubernetes/pkg/api/v1"
	kubeclientset "k8s.io/kubernetes/pkg/client/clientset_generated/clientset"
	//"k8s.io/apimachinery/pkg/api/resource"
)

const (
	ResourceQuotaKind           = "resourcequota"
	ResourceQuotaControllerName = "resourcequotas"
)

func init() {
	RegisterFederatedType(ResourceQuotaKind, ResourceQuotaControllerName, []schema.GroupVersionResource{apiv1.SchemeGroupVersion.WithResource(ResourceQuotaControllerName)}, NewResourceQuotaAdapter)
}

type ResourceQuotaAdapter struct {
	client federationclientset.Interface
}

func NewResourceQuotaAdapter(client federationclientset.Interface) FederatedTypeAdapter {
	return &ResourceQuotaAdapter{client: client}
}

func (a *ResourceQuotaAdapter) Kind() string {
	return ResourceQuotaKind
}

func (a *ResourceQuotaAdapter) ObjectType() pkgruntime.Object {
	return &apiv1.ResourceQuota{}
}

func (a *ResourceQuotaAdapter) IsExpectedType(obj interface{}) bool {
	_, ok := obj.(*apiv1.ResourceQuota)
	return ok
}

func (a *ResourceQuotaAdapter) Copy(obj pkgruntime.Object) pkgruntime.Object {
	resourcequota := obj.(*apiv1.ResourceQuota)
	return &apiv1.ResourceQuota{
		ObjectMeta: util.DeepCopyRelevantObjectMeta(resourcequota.ObjectMeta),
		Spec:       resourcequota.Spec,
	}
}

func (a *ResourceQuotaAdapter) Equivalent(obj1, obj2 pkgruntime.Object) bool {
	resourcequota1 := obj1.(*apiv1.ResourceQuota)
	resourcequota2 := obj2.(*apiv1.ResourceQuota)
	return util.ResourceQuotaEquivalent(resourcequota1, resourcequota2)
}

func (a *ResourceQuotaAdapter) NamespacedName(obj pkgruntime.Object) types.NamespacedName {
	resourcequota := obj.(*apiv1.ResourceQuota)
	return types.NamespacedName{Namespace: resourcequota.Namespace, Name: resourcequota.Name}
}

func (a *ResourceQuotaAdapter) ObjectMeta(obj pkgruntime.Object) *metav1.ObjectMeta {
	return &obj.(*apiv1.ResourceQuota).ObjectMeta
}

func (a *ResourceQuotaAdapter) FedCreate(obj pkgruntime.Object) (pkgruntime.Object, error) {
	resourcequota := obj.(*apiv1.ResourceQuota)
	return a.client.CoreV1().ResourceQuotas(resourcequota.Namespace).Create(resourcequota)
}

func (a *ResourceQuotaAdapter) FedDelete(namespacedName types.NamespacedName, options *metav1.DeleteOptions) error {
	return a.client.CoreV1().ResourceQuotas(namespacedName.Namespace).Delete(namespacedName.Name, options)
}

func (a *ResourceQuotaAdapter) FedGet(namespacedName types.NamespacedName) (pkgruntime.Object, error) {
	return a.client.CoreV1().ResourceQuotas(namespacedName.Namespace).Get(namespacedName.Name, metav1.GetOptions{})
}

func (a *ResourceQuotaAdapter) FedList(namespace string, options metav1.ListOptions) (pkgruntime.Object, error) {
	return a.client.CoreV1().ResourceQuotas(namespace).List(options)
}

func (a *ResourceQuotaAdapter) FedUpdate(obj pkgruntime.Object) (pkgruntime.Object, error) {
	resourcequota := obj.(*apiv1.ResourceQuota)
	return a.client.CoreV1().ResourceQuotas(resourcequota.Namespace).Update(resourcequota)
}

func (a *ResourceQuotaAdapter) FedWatch(namespace string, options metav1.ListOptions) (watch.Interface, error) {
	return a.client.CoreV1().ResourceQuotas(namespace).Watch(options)
}

func (a *ResourceQuotaAdapter) ClusterCreate(client kubeclientset.Interface, obj pkgruntime.Object) (pkgruntime.Object, error) {
	resourcequota := obj.(*apiv1.ResourceQuota)
	return client.CoreV1().ResourceQuotas(resourcequota.Namespace).Create(resourcequota)
}

func (a *ResourceQuotaAdapter) ClusterDelete(client kubeclientset.Interface, nsName types.NamespacedName, options *metav1.DeleteOptions) error {
	return client.CoreV1().ResourceQuotas(nsName.Namespace).Delete(nsName.Name, options)
}

func (a *ResourceQuotaAdapter) ClusterGet(client kubeclientset.Interface, namespacedName types.NamespacedName) (pkgruntime.Object, error) {
	return client.CoreV1().ResourceQuotas(namespacedName.Namespace).Get(namespacedName.Name, metav1.GetOptions{})
}

func (a *ResourceQuotaAdapter) ClusterList(client kubeclientset.Interface, namespace string, options metav1.ListOptions) (pkgruntime.Object, error) {
	return client.CoreV1().ResourceQuotas(namespace).List(options)
}

func (a *ResourceQuotaAdapter) ClusterUpdate(client kubeclientset.Interface, obj pkgruntime.Object) (pkgruntime.Object, error) {
	resourcequota := obj.(*apiv1.ResourceQuota)
	return client.CoreV1().ResourceQuotas(resourcequota.Namespace).Update(resourcequota)
}

func (a *ResourceQuotaAdapter) ClusterWatch(client kubeclientset.Interface, namespace string, options metav1.ListOptions) (watch.Interface, error) {
	return client.CoreV1().ResourceQuotas(namespace).Watch(options)
}

func (a *ResourceQuotaAdapter) IsSchedulingAdapter() bool {
	return false
}

func (a *ResourceQuotaAdapter) NewTestObject(namespace string) pkgruntime.Object {
	return &apiv1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "test-resourcequota-",
			Namespace:    namespace,
		},
		/*
		Spec: {apiv1.ResourceList{
				"A": "1",
			},
			[]apiv1.ResourceQuotaScope{
				apiv1.ResourceQuotaScopeBestEffort,
			},
		},
		Status: {
			apiv1.ResourceList{
				"A": "1",
			},
			apiv1.ResourceList {
				"A": "1",
			}
		}*/
	}
}
