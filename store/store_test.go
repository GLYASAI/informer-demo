package store

import (
	"fmt"
	"github.com/eapache/channels"
	"k8s.io/api/core/v1"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"sync/atomic"
	"testing"
	"time"
)

func TestStore(t *testing.T) {
	clientset := fake.NewSimpleClientset()

	t.Run("should return one event for add, update and delete of pod", func(t *testing.T) {
		ns := createNamespace(clientset, t)
		defer deleteNamespace(ns, clientset, t)

		stopCh := make(chan struct{})
		updateCh := channels.NewRingChannel(1024)

		var add uint64
		var upd uint64
		var del uint64

		go func(ch *channels.RingChannel) {
			for {
				evt, ok := <-ch.Out()
				if !ok {
					return
				}

				e := evt.(Event)
				if e.Obj == nil {
					continue
				}
				if _, ok := e.Obj.(*corev1.Pod); !ok {
					continue
				}

				switch e.Type {
				case CreateEvent:
					atomic.AddUint64(&add, 1)
				case UpdateEvent:
					atomic.AddUint64(&upd, 1)
				case DeleteEvent:
					atomic.AddUint64(&del, 1)
				}
			}
		}(updateCh)

		s := New(clientset, time.Duration(10), ns, updateCh)
		s.Run(stopCh)

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "dummy-pod",
				Namespace: ns,
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "nginx",
						Image: "nginx",
						Ports: []corev1.ContainerPort{
							{
								ContainerPort: 80,
								Protocol:      corev1.ProtocolTCP,
							},
						},
					},
				},
			},
		}
		_ = ensurePod(pod, clientset, t)

		pod.Spec.Containers[0].Ports[0].ContainerPort = 81
		_ = ensurePod(pod, clientset, t)

		err := clientset.CoreV1().Pods(pod.Namespace).Delete(pod.Name, &metav1.DeleteOptions{})
		if err != nil {
			t.Errorf("error deleting pod: %v", err)
		}

		time.Sleep(1 * time.Second)

		if atomic.LoadUint64(&add) != 1 {
			t.Errorf("expected 1 event of type Create but %v occurred", add)
		}
		if atomic.LoadUint64(&upd) != 1 {
			t.Errorf("expected 1 event of type Update but %v occurred", upd)
		}
		if atomic.LoadUint64(&del) != 1 {
			t.Errorf("expected 1 event of type Delete but %v occurred", del)
		}
	})
}

func TestStore_ListPods(t *testing.T) {
	clientset := fake.NewSimpleClientset()

	updateCh := channels.NewRingChannel(10)
	ns := corev1.NamespaceDefault
	s := New(clientset, time.Duration(10), ns, updateCh)
	_ = s.listers.Pod.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dummy-pod-1",
			Namespace: ns,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 80,
							Protocol:      corev1.ProtocolTCP,
						},
					},
				},
			},
		},
	})
	_ = s.listers.Pod.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dummy-pod-2",
			Namespace: ns,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 80,
							Protocol:      corev1.ProtocolTCP,
						},
					},
				},
			},
		},
	})
	_ = s.listers.Pod.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dummy-pod-3",
			Namespace: ns,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 80,
							Protocol:      corev1.ProtocolTCP,
						},
					},
				},
			},
		},
	})

	time.Sleep(2 * time.Second)

	pods := s.ListPods()
	if s := len(pods); s != 3 {
		t.Errorf("Expected 3 Pods but got %v", s)
	}
}

func TestStore_GetPod(t *testing.T) {
	clientset := fake.NewSimpleClientset()

	updateCh := channels.NewRingChannel(10)
	ns := corev1.NamespaceDefault
	s := New(clientset, time.Duration(10), ns, updateCh)
	_ = s.listers.Pod.Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dummy-pod",
			Namespace: ns,
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "nginx",
					Image: "nginx",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 80,
							Protocol:      corev1.ProtocolTCP,
						},
					},
				},
			},
		},
	})

	_, err := s.listers.Pod.ByKey(fmt.Sprintf("%s/%s", corev1.NamespaceDefault, "dummy-pod"))
	if err != nil {
		t.Fatalf("error creating pod: %v", err)
	}

}

func createNamespace(clientSet kubernetes.Interface, t *testing.T) string {
	t.Helper()
	t.Log("Creating temporal namespace")

	namespace := &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "store-test",
		},
	}

	ns, err := clientSet.CoreV1().Namespaces().Create(namespace)
	if err != nil {
		t.Errorf("error creating the namespace: %v", err)
	}
	t.Logf("Temporal namespace %v created", ns)

	return ns.Name
}

func deleteNamespace(ns string, clientSet kubernetes.Interface, t *testing.T) {
	t.Helper()
	t.Logf("Deleting temporal namespace %v", ns)

	err := clientSet.CoreV1().Namespaces().Delete(ns, &metav1.DeleteOptions{})
	if err != nil {
		t.Errorf("error deleting the namespace: %v", err)
	}
	t.Logf("Temporal namespace %v deleted", ns)
}

func ensurePod(pod *corev1.Pod, clientSet kubernetes.Interface, t *testing.T) *corev1.Pod {
	t.Helper()
	p, err := clientSet.CoreV1().Pods(pod.Namespace).Update(pod)

	if err != nil {
		if k8sErrors.IsNotFound(err) {
			t.Logf("Pod %v not found, creating", pod)

			p, err = clientSet.CoreV1().Pods(pod.Namespace).Create(pod)
			if err != nil {
				t.Fatalf("error creating pod %+v: %v", pod, err)
			}

			t.Logf("Pod %+v created", pod)
			return p
		}

		t.Fatalf("error updating pod %+v: %v", pod, err)
	}

	t.Logf("Pod %+v updated", pod)

	return pod
}
