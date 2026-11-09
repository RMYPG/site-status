// Package watch keeps a read-only view of the cluster objects the service
// reports on. It runs one informer per resource and namespace, and never lists
// or watches across the cluster: the service's RBAC is granted per namespace,
// so a cluster-scoped call would fail in production.
package watch

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RMYPG/site-status/internal/config"
	"github.com/RMYPG/site-status/internal/rollout"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

// FluxNamespace holds the image automation objects and the Kustomizations.
const FluxNamespace = "flux-system"

const (
	revisionAnnotation = "deployment.kubernetes.io/revision"
	// The rules look at the clock on every request, so nothing needs a periodic
	// resync: a watch event is the only reason an object changes.
	noResync = time.Duration(0)
	// watchErrorWindow is how long one reported watch failure keeps the service
	// unready. The reflector's retries back off to between 30 and 60 seconds, so
	// a watch that keeps failing renews it and one that has recovered lets it
	// lapse. client-go retries connection refused and 429 when opening a watch
	// without calling the error handler, so those two don't count.
	watchErrorWindow = 2 * time.Minute
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

// retrying lists the resources that retry on their own, whose failures the
// rules judge by how long a not-Ready episode has lasted.
var retrying = map[schema.GroupVersionResource]bool{
	imageRepositories:      true,
	imagePolicies:          true,
	imageUpdateAutomations: true,
	kustomizations:         true,
}

type informerKey struct {
	namespace string
	resource  schema.GroupVersionResource
}

type episodeKey struct {
	resource        schema.GroupVersionResource
	namespace, name string
}

// Watcher reads the configured objects from informer caches. It holds one thing
// the cluster can't tell it: how long each retrying object has gone without
// becoming Ready.
type Watcher struct {
	factories     []dynamicinformer.DynamicSharedInformerFactory
	informers     map[informerKey]informers.GenericInformer
	registrations []cache.ResourceEventHandlerRegistration
	automation    string
	now           func() time.Time
	log           *slog.Logger
	// track handles one informer event. Tests wrap it to delay the handler.
	track func(schema.GroupVersionResource, any)

	episodesMu sync.RWMutex
	episodes   map[episodeKey]*rollout.Episode

	// lastWatchError is the UnixNano of the latest watch failure, zero if none.
	lastWatchError atomic.Int64
}

// New prepares one informer per resource and namespace. Nothing runs until Start.
func New(client dynamic.Interface, cfg *config.Config, now func() time.Time, log *slog.Logger) *Watcher {
	w := &Watcher{
		informers:  map[informerKey]informers.GenericInformer{},
		automation: cfg.ImageUpdateAutomation,
		now:        now,
		log:        log,
		episodes:   map[episodeKey]*rollout.Episode{},
	}
	w.track = w.trackEpisode

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
		shared := informer.Informer()
		// These fail only on an informer that has started, and this one hasn't.
		_ = shared.SetTransform(dropManagedFields)
		_ = shared.SetWatchErrorHandler(func(_ *cache.Reflector, err error) {
			w.recordWatchError(resource, namespace, err)
		})
		if retrying[resource] {
			registration, _ := shared.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc:    func(object any) { w.track(resource, object) },
				UpdateFunc: func(_, object any) { w.track(resource, object) },
				DeleteFunc: func(object any) { w.forgetEpisode(resource, object) },
			})
			w.registrations = append(w.registrations, registration)
		}
		w.informers[informerKey{namespace, resource}] = informer
	}
}

// Start begins watching. It returns immediately; the watches stop with ctx.
func (w *Watcher) Start(ctx context.Context) {
	for _, factory := range w.factories {
		factory.Start(ctx.Done())
	}
}

// Synced reports whether every watch has completed its first list, and the
// episodes have seen it. It never reverts: see WatchFailing.
func (w *Watcher) Synced() bool {
	for _, informer := range w.informers {
		if !informer.Informer().HasSynced() {
			return false
		}
	}
	for _, registration := range w.registrations {
		if !registration.HasSynced() {
			return false
		}
	}
	return true
}

// WatchFailing reports whether client-go reported a watch failure within
// watchErrorWindow, which means the caches may have stopped following the
// cluster.
func (w *Watcher) WatchFailing() bool {
	last := w.lastWatchError.Load()
	return last != 0 && w.now().Sub(time.Unix(0, last)) < watchErrorWindow
}

// recordWatchError logs a broken watch and notes when it happened. The routine
// ends of a watch are left out, as in client-go's default handler: the server
// closes watches regularly and the reflector just opens another.
func (w *Watcher) recordWatchError(resource schema.GroupVersionResource, namespace string, err error) {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
		return
	}
	w.lastWatchError.Store(w.now().UnixNano())
	w.log.Warn("watch failed", "resource", resource.Resource, "namespace", namespace, "error", err)
}

// trackEpisode updates an object's not-Ready episode. It runs on every informer
// event, so a change between two requests isn't missed.
func (w *Watcher) trackEpisode(resource schema.GroupVersionResource, object any) {
	item, ok := object.(*unstructured.Unstructured)
	if !ok {
		return
	}
	ready := readyCondition(conditions(item))
	key := episodeKey{resource, item.GetNamespace(), item.GetName()}

	w.episodesMu.Lock()
	defer w.episodesMu.Unlock()
	if next := rollout.NextEpisode(w.episodes[key], string(item.GetUID()), ready, w.now()); next != nil {
		w.episodes[key] = next
	} else {
		delete(w.episodes, key)
	}
}

func (w *Watcher) forgetEpisode(resource schema.GroupVersionResource, object any) {
	if tombstone, ok := object.(cache.DeletedFinalStateUnknown); ok {
		object = tombstone.Obj
	}
	item, ok := object.(*unstructured.Unstructured)
	if !ok {
		return
	}
	w.episodesMu.Lock()
	defer w.episodesMu.Unlock()
	delete(w.episodes, episodeKey{resource, item.GetNamespace(), item.GetName()})
}

func (w *Watcher) episode(resource schema.GroupVersionResource, namespace, name string) *rollout.Episode {
	w.episodesMu.RLock()
	defer w.episodesMu.RUnlock()
	return w.episodes[episodeKey{resource, namespace, name}]
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
	object.NotReady = w.currentEpisode(resource, item, object.Conditions)
	return item, object
}

// currentEpisode returns the episode that describes this cached object. The
// informer updates its cache before it runs the event handler, so the tracked
// episode can still belong to an earlier version of the object: one with another
// UID, or one that Ready=True hasn't cleared yet. An object without a matching
// episode is judged from its own lastTransitionTime, as on seeding.
func (w *Watcher) currentEpisode(resource schema.GroupVersionResource, item *unstructured.Unstructured, list []rollout.Condition) *rollout.Episode {
	if !retrying[resource] {
		return nil
	}
	ready := readyCondition(list)
	uid := string(item.GetUID())
	tracked := w.episode(resource, item.GetNamespace(), item.GetName())
	if tracked != nil && tracked.UID == uid && (ready == nil || ready.Status != "True") {
		return tracked
	}
	return rollout.NextEpisode(nil, uid, ready, w.now())
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

func readyCondition(list []rollout.Condition) *rollout.Condition {
	for i := range list {
		if list[i].Type == "Ready" {
			return &list[i]
		}
	}
	return nil
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
