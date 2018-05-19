package main

import (
	"fmt"
	"time"

	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

const controllerAgentName = "namespace-resource-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Namespace is synced
	SuccessSynced = "Synced"

	// MessageResourceSynced is the message used for an Event fired when a Namespace
	// is synced successfully
	MessageResourceSynced = "Namespace synced successfully"
)

// Controller is the controller implementation for Namespace resources
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface

	namespacesLister corelisters.NamespaceLister
	namespacesSynced cache.InformerSynced
	resourceLister   corelisters.ResourceQuotaLister
	limitLister      corelisters.LimitRangeLister

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

// NewController returns a new Namespace resource controller
func NewController(
	kubeclientset kubernetes.Interface,
	kubeInformerFactory kubeinformers.SharedInformerFactory) *Controller {

	// obtain references to shared index informers for Namespaces, resourcequotas, and limitranges
	namespaceInformer := kubeInformerFactory.Core().V1().Namespaces()
	resourceInformer := kubeInformerFactory.Core().V1().ResourceQuotas()
	limitInformer := kubeInformerFactory.Core().V1().LimitRanges()

	recorder := newEventBroadcaster(kubeclientset)

	controller := &Controller{
		kubeclientset:    kubeclientset,
		namespacesLister: namespaceInformer.Lister(),
		namespacesSynced: namespaceInformer.Informer().HasSynced,
		resourceLister:   resourceInformer.Lister(),
		limitLister:      limitInformer.Lister(),
		workqueue:        workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Namespaces"),
		recorder:         recorder,
	}

	glog.Info("Setting up event handlers")

	// Set up namesspace lister
	namespaceInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueNs,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueNs(new)
		},
	})

	return controller
}

func newEventBroadcaster(kubeclientset kubernetes.Interface) record.EventRecorder {
	glog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	return eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	glog.Infof("Starting %s", controllerAgentName)

	// Wait for the caches to be synced before starting workers
	glog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.namespacesSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("Starting workers")
	// Launch two workers to process Namespace resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	glog.Info("Started workers")
	<-stopCh
	glog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// namespace to be synced.
		if err := c.syncHandler(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		glog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two.
func (c *Controller) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name,
	// as we only have namespaces, we'll just be interested in the name.
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the namespace resource with this name
	ns, err := c.namespacesLister.Get(name)
	if err != nil {
		// The namespace resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("namespace '%s' in work queue no longer exists", key))
			return nil
		}

		return err
	}

	glog.Infof("processing ns %s", ns.Name)

	resourceQuotas, err := c.resourceLister.ResourceQuotas(ns.Name).List(labels.Everything())
	if err != nil {
		return err
	}
	limitRanges, err := c.limitLister.LimitRanges(ns.Name).List(labels.Everything())
	if err != nil {
		return err
	}

	if ns.Annotations["resource-exemption"] == "true" {
		glog.Infof("Namespace '%s' using custom quotas and limits. Skipping...", ns.Name)
		return nil
	}
	if len(resourceQuotas) == 0 {
		glog.Infof("No resource quotas defined for '%s'. Creating default...", ns.Name)
		rq, err := c.kubeclientset.Core().ResourceQuotas(ns.Name).Create(newDefaultResourceQuota(ns.Name))
		if err != nil {
			return err
		}
		glog.Infof("Created default resource quota for namespace '%s': \r\n%s", ns.Name, rq)
	}

	if len(limitRanges) == 0 {
		glog.Infof("No limit ranges defined for '%s'. Creating default...", ns.Name)
		lr, err := c.kubeclientset.Core().LimitRanges(ns.Name).Create(newDefaultLimitRange(ns.Name))
		if err != nil {
			return err
		}
		glog.Infof("Created default limit range for namespace '%s': \r\n%+v", ns.Name, lr.Spec)
	}

	c.recorder.Event(ns, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

// enqueueNs takes a Namespace resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Namespace.
func (c *Controller) enqueueNs(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.AddRateLimited(key)
}

func newDefaultResourceQuota(namespace string) *corev1.ResourceQuota {
	cpuLimits, _ := resource.ParseQuantity("2")
	cpuRequests, _ := resource.ParseQuantity("1")
	memoryLimits, _ := resource.ParseQuantity("2Gi")
	memoryRequests, _ := resource.ParseQuantity("1Gi")
	quotas := map[corev1.ResourceName]resource.Quantity{
		"limits.cpu":      cpuLimits,
		"limits.memory":   memoryLimits,
		"requests.cpu":    cpuRequests,
		"requests.memory": memoryRequests,
	}
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespace,
			Namespace: namespace,
		},
		Spec: corev1.ResourceQuotaSpec{
			Hard: quotas,
		},
	}
}

func newDefaultLimitRange(namespace string) *corev1.LimitRange {
	cpuLimit, _ := resource.ParseQuantity("1")
	cpuRequest, _ := resource.ParseQuantity("500m")
	memoryLimit, _ := resource.ParseQuantity("512Mi")
	memoryRequest, _ := resource.ParseQuantity("256Mi")
	defaultLimit := map[corev1.ResourceName]resource.Quantity{
		"cpu":    cpuLimit,
		"memory": memoryLimit,
	}
	defaultRequest := map[corev1.ResourceName]resource.Quantity{
		"cpu":    cpuRequest,
		"memory": memoryRequest,
	}
	return &corev1.LimitRange{
		ObjectMeta: metav1.ObjectMeta{
			Name:      namespace,
			Namespace: namespace,
		},
		Spec: corev1.LimitRangeSpec{
			Limits: []corev1.LimitRangeItem{
				{
					Default:        defaultLimit,
					DefaultRequest: defaultRequest,
					Type:           "Container",
				},
			},
		},
	}
}
