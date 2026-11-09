// Package watch keeps a read-only view of the cluster objects the service
// reports on. It runs one informer per resource and namespace, and never lists
// or watches across the cluster: the service's RBAC is granted per namespace,
// so a cluster-scoped call would fail in production.
package watch

import (
	"context"
	"strconv"
	"time"

	"github.com/RMYPG/site-status/internal/config"
	"github.com/RMYPG/site-status/internal/rollout"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
)

// FluxNamespace holds the image automation objects and the Kustomizations.
const FluxNamespace = "flux-system"

const (
	revisionAnnotation = "deployment.kubernetes.io/revision"
	// The rules look at the clock on every request, so nothing needs a periodic
	// resync: a watch event is the only reason an object changes.
	noResync = time.Duration(0)
)

// The resources the service reads. The dynamic client reads them as
// unstructured objects, so the Flux API modules aren't dependencies.
var (
	deployments            = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
	replicaSets            = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
	helmReleases           = schema.GroupVersionResource{Group: "helm.toolkit.fluxcd.io", Version: "v2", Resource: "helmreleases"}
	imageRepositories      = schema.GroupVersionResource{Group: "image.toolkit.fluxcd.io", Version: "v1", Resource: "imagerepositories"}
	imagePolicies          = schema.GroupVersionResource{Group: "image.toolkit.fluxcd.io", Version: "v1", Resource: "imagepolicies"}
	imageUpdateAutomations = schema.GroupVersionResource{Group: "image.toolkit.fluxcd.io", Version: "v1", Resource: "imageupdateautomations"}
	kustomizations         = schema.GroupVersionResource{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Resource: "kustomizations"}
)

type informerKey struct {
	namespace string
	resource  schema.GroupVersionResource
}

// Watcher reads the configured objects from informer caches.
type Watcher struct {
	factories  []dynamicinformer.DynamicSharedInformerFactory
	informers  map[informerKey]informers.GenericInformer
	automation string
}

// New prepares one informer per resource and namespace. Nothing runs until Start.
func New(client dynamic.Interface, cfg *config.Config) *Watcher {
	w := &Watcher{
		informers:  map[informerKey]informers.GenericInformer{},
		automation: cfg.ImageUpdateAutomation,
	}

	w.addNamespace(client, FluxNamespace, imageRepositories, imagePolicies, imageUpdateAutomations, kustomizations)
	seen := map[string]bool{}
	for _, site := range cfg.Sites {
		for _, env := range site.Environments {
			if seen[env.Namespace] {
				continue
			}
			seen[env.Namespace] = true
			w.addNamespace(client, env.Namespace, deployments, replicaSets, helmReleases)
		}
	}
	return w
}

func (w *Watcher) addNamespace(client dynamic.Interface, namespace string, resources ...schema.GroupVersionResource) {
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(client, noResync, namespace, nil)
	w.factories = append(w.factories, factory)
	for _, resource := range resources {
		informer := factory.ForResource(resource)
		// SetTransform fails only on an informer that has started, and this one
		// hasn't.
		_ = informer.Informer().SetTransform(dropManagedFields)
		w.informers[informerKey{namespace, resource}] = informer
	}
}

// Start begins watching. It returns immediately; the watches stop with ctx.
func (w *Watcher) Start(ctx context.Context) {
	for _, factory := range w.factories {
		factory.Start(ctx.Done())
	}
}

// Synced reports whether every watch has completed its first list.
func (w *Watcher) Synced() bool {
	for _, informer := range w.informers {
		if !informer.Informer().HasSynced() {
			return false
		}
	}
	return true
}

// Observe reads one environment's objects from the caches. Call it only once
// Synced is true: before that, a missing object means nothing.
func (w *Watcher) Observe(env config.Environment) rollout.Observation {
	deployment, deploymentObject := w.object(deployments, "Deployment", env.Namespace, env.Deployment)
	_, helmRelease := w.object(helmReleases, "HelmRelease", env.Namespace, env.HelmRelease)
	_, automation := w.object(imageUpdateAutomations, "ImageUpdateAutomation", FluxNamespace, w.automation)
	_, kustomization := w.object(kustomizations, "Kustomization", FluxNamespace, env.Kustomization)
	_, imageRepository := w.object(imageRepositories, "ImageRepository", FluxNamespace, env.ImageRepository)
	policy, imagePolicy := w.object(imagePolicies, "ImagePolicy", FluxNamespace, env.ImagePolicy)

	observation := rollout.Observation{
		Deployment:            deploymentObject,
		HelmRelease:           helmRelease,
		ImageUpdateAutomation: automation,
		Kustomization:         kustomization,
		ImageRepository:       imageRepository,
		ImagePolicy:           imagePolicy,
	}
	if policy != nil {
		observation.Target, _, _ = unstructured.NestedString(policy.Object, "status", "latestRef", "tag")
	}
	if deployment != nil {
		observation.ReplicaSets = w.replicaSetsOf(env.Namespace, deployment.GetUID())
	}
	return observation
}

func (w *Watcher) object(resource schema.GroupVersionResource, kind, namespace, name string) (*unstructured.Unstructured, rollout.Object) {
	object := rollout.Object{Kind: kind, Namespace: namespace, Name: name}
	informer, ok := w.informers[informerKey{namespace, resource}]
	if !ok {
		return nil, object
	}
	found, err := informer.Lister().ByNamespace(namespace).Get(name)
	if err != nil {
		return nil, object
	}
	item, ok := found.(*unstructured.Unstructured)
	if !ok {
		return nil, object
	}
	object.Found = true
	object.Conditions = conditions(item)
	return item, object
}

// replicaSetsOf returns the ReplicaSets the Deployment controls, matched by the
// UID in their owner reference and not by labels or names.
func (w *Watcher) replicaSetsOf(namespace string, deploymentUID types.UID) []rollout.ReplicaSet {
	informer := w.informers[informerKey{namespace, replicaSets}]
	objects, err := informer.Lister().ByNamespace(namespace).List(labels.Everything())
	if err != nil {
		return nil
	}

	var result []rollout.ReplicaSet
	for _, object := range objects {
		item, ok := object.(*unstructured.Unstructured)
		if !ok {
			continue
		}
		owner := metav1.GetControllerOf(item)
		if owner == nil || owner.UID != deploymentUID {
			continue
		}
		available, _, _ := unstructured.NestedInt64(item.Object, "status", "availableReplicas")
		revision, _ := strconv.ParseInt(item.GetAnnotations()[revisionAnnotation], 10, 64)
		result = append(result, rollout.ReplicaSet{
			Name:              item.GetName(),
			Image:             firstContainerImage(item),
			CreatedAt:         item.GetCreationTimestamp().UTC(),
			Revision:          revision,
			AvailableReplicas: available,
		})
	}
	return result
}

// firstContainerImage reads the image of the pod template's first container,
// which is the Site's server in the sws chart.
func firstContainerImage(item *unstructured.Unstructured) string {
	containers, _, _ := unstructured.NestedSlice(item.Object, "spec", "template", "spec", "containers")
	if len(containers) == 0 {
		return ""
	}
	container, ok := containers[0].(map[string]any)
	if !ok {
		return ""
	}
	image, _, _ := unstructured.NestedString(container, "image")
	return image
}

func conditions(item *unstructured.Unstructured) []rollout.Condition {
	raw, _, _ := unstructured.NestedSlice(item.Object, "status", "conditions")
	result := make([]rollout.Condition, 0, len(raw))
	for _, entry := range raw {
		condition, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		transition, _ := condition["lastTransitionTime"].(string)
		// A missing or malformed time stays zero, which the rules treat as unknown.
		parsed, _ := time.Parse(time.RFC3339, transition)
		result = append(result, rollout.Condition{
			Type:               stringField(condition, "type"),
			Status:             stringField(condition, "status"),
			Reason:             stringField(condition, "reason"),
			Message:            stringField(condition, "message"),
			LastTransitionTime: parsed,
		})
	}
	return result
}

func stringField(fields map[string]any, key string) string {
	value, _ := fields[key].(string)
	return value
}

// dropManagedFields frees most of the memory an informer holds per object; the
// rules never read them.
func dropManagedFields(object any) (any, error) {
	if accessor, ok := object.(metav1.Object); ok {
		accessor.SetManagedFields(nil)
	}
	return object, nil
}
