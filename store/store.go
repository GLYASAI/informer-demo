package store

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/eapache/channels"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"reflect"
	"time"
)

// EventType type of event associated with an informer
type EventType string

const (
	// CreateEvent event associated with new objects in an informer
	CreateEvent EventType = "CREATE"
	// UpdateEvent event associated with an object update in an informer
	UpdateEvent EventType = "UPDATE"
	// DeleteEvent event associated when an object is removed from an informer
	DeleteEvent EventType = "DELETE"
)

type Storer interface {
	ListPods() []*corev1.Pod
	GetPod(key string) (*corev1.Pod, error)

	// Run initiates the synchronization of the controllers
	Run(stopCh chan struct{})
}

type Store struct {
	// informer contains the cache Informers
	informers *Informer
	// listers contains the cache.Store interfaces
	listers *Lister

	updateCh *channels.RingChannel
}

// Event holds the context of an event.
type Event struct {
	Type EventType
	Obj  interface{}
}

// Informer defines the required SharedIndexInformers that interact with the API server.
type Informer struct {
	Pod cache.SharedIndexInformer
}

// Run initiates the synchronization of the informers against the API server.
func (i *Informer) Run(stopCh chan struct{}) {
	go i.Pod.Run(stopCh)

	// wait for all involved caches to be synced before processing items
	// from the queue
	if !cache.WaitForCacheSync(stopCh,
		i.Pod.HasSynced,
	) {
		runtime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
	}
}

// Lister contains object listers (stores).
type Lister struct {
	Pod PodLister
}

// NotExistsError is returned when an object does not exist in a local store.
type NotExistsError string

// Error implements the error interface.
func (e NotExistsError) Error() string {
	return fmt.Sprintf("no object matching key %q in local store", string(e))
}

// New creates a new object store to be used in controller.
func New(client clientset.Interface,
	resyncPeriod time.Duration,
	namespace string,
	updateCh *channels.RingChannel) *Store {
	s := &Store{
		informers: &Informer{},
		listers:   &Lister{},
		updateCh:  updateCh,
	}

	// create informers factory, enable and assign required informers
	infFactory := informers.NewSharedInformerFactoryWithOptions(client, resyncPeriod,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(*metav1.ListOptions) {}))

	s.informers.Pod = infFactory.Core().V1().Pods().Informer()
	s.listers.Pod.Store = s.informers.Pod.GetStore()

	podEventHandler := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			updateCh.In() <- Event{
				Type: CreateEvent,
				Obj:  obj,
			}
		},
		UpdateFunc: func(old, new interface{}) {
			o, ok := old.(*corev1.Pod)
			if !ok {
				logrus.Warningf("cant not convert %s to *corev1.Pod", reflect.TypeOf(old))
			}
			n, ok := new.(*corev1.Pod)
			if !ok {
				logrus.Warningf("cant not convert %s to *corev1.Pod", reflect.TypeOf(new))
			}

			// no changes between the old pod and new the pod
			if o.ObjectMeta.ResourceVersion == n.ObjectMeta.ResourceVersion && reflect.DeepEqual(o, n) {
				return
			}

			updateCh.In() <- Event{
				Type: UpdateEvent,
				Obj:  new,
			}
		},
		DeleteFunc: func(obj interface{}) {
			updateCh.In() <- Event{
				Type: DeleteEvent,
				Obj:  obj,
			}
		},
	}

	s.informers.Pod.AddEventHandlerWithResyncPeriod(podEventHandler, resyncPeriod)

	return s
}

// ListPods returns the list of Pods.
func (s *Store) ListPods() []*corev1.Pod {
	result := make([]*corev1.Pod, 0)
	for _, item := range s.listers.Pod.List() {
		pod := item.(*corev1.Pod)
		result = append(result, pod)
	}
	return result
}

// GetPod returns Pod matching key.
func (s *Store) GetPod(key string) (*corev1.Pod, error) {
	return s.listers.Pod.ByKey(key)
}

// Run initiates the synchronization of the informers and the initial
// synchronization of the secrets.
func (s *Store) Run(stopCh chan struct{}) {
	// start informers
	s.informers.Run(stopCh)
}
