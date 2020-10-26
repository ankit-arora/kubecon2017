package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/sirupsen/logrus"
	api "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var clientset *kubernetes.Clientset

var controller cache.Controller
var store cache.Store
var imageCapacity map[string]int64

func main() {
	var err error
	clientset, err = getClient("/Users/ankit/.kube/config")
	if err != nil {
		logrus.Error(err)
		logrus.Fatal(err)
	}
	podList, err := clientset.CoreV1().Pods("default").List(context.TODO(), v1.ListOptions{})
	if err != nil {
		logrus.Error(err)
		logrus.Fatal(err)
	}
	for _, pod := range podList.Items {
		fmt.Println(pod.Name)
	}
	//watchNodes()
	//pollNodes()
	//app := cli.NewApp()
	//app.Flags = []cli.Flag{
	//	cli.StringFlag{
	//		Name:  "config",
	//		Usage: "Kube config path for outside of cluster access",
	//	},
	//}
	//
	//app.Action = func(c *cli.Context) error {
	//	var err error
	//	clientset, err = getClient(c.String("config"))
	//	if err != nil {
	//		logrus.Error(err)
	//		return err
	//	}
	//	go pollNodes()
	//	//watchNodes()
	//	for {
	//		time.Sleep(5 * time.Second)
	//	}
	//}
	//app.Run(os.Args)
}

func watchNodes() {
	imageCapacity = make(map[string]int64)
	//Regular informer example
	watchList := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "nodes", v1.NamespaceAll,
		fields.Everything())
	store, controller = cache.NewInformer(
		watchList,
		&api.Node{},
		time.Second*30,
		cache.ResourceEventHandlerFuncs{
			AddFunc:    handleNodeAdd,
			UpdateFunc: handleNodeUpdate,
		},
	)

	stop := make(chan struct{})
	go controller.Run(stop)

	// Shared informer example
	informer := cache.NewSharedIndexInformer(
		watchList,
		&api.Node{},
		time.Second*10,
		cache.Indexers{},
	)

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handleNodeAdd,
		UpdateFunc: handleNodeUpdate,
	})

	// More than one handler can be added...
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    handleNodeAdd,
		UpdateFunc: handleNodeUpdate,
	})
}

func handleNodeAdd(obj interface{}) {
	node := obj.(*api.Node)
	logrus.Infof("Node [%s] is added; checking resources...", node.Name)
	checkImageStorage(node)
}

func handleNodeUpdate(old, current interface{}) {
	// Cache access example
	nodeInterface, exists, err := store.GetByKey("minikube")
	if exists && err == nil {
		logrus.Debugf("Found the node [%v] in cache", nodeInterface)
	}

	node := current.(*api.Node)
	checkImageStorage(node)
}

func pollNodes() error {
	for {
		nodes, err := clientset.CoreV1().Nodes().List(context.TODO(), v1.ListOptions{FieldSelector: "metadata.name=minikube"})
		if err != nil {
			logrus.Warnf("Failed to poll the nodes: %v", err)
			continue
		}
		if len(nodes.Items) > 0 {
			node := nodes.Items[0]
			node.Annotations["checked"] = "true"
			_, err := clientset.CoreV1().Nodes().Update(context.TODO(), &node, v1.UpdateOptions{})
			if err != nil {
				logrus.Warnf("Failed to update the node: %v", err)
				continue
			}
			// // Node removal example
			// gracePeriod := int64(10)
			// err = clientset.Core().Nodes().Delete(updatedNode.Name,
			// 	&v1.DeleteOptions{GracePeriodSeconds: &gracePeriod})
		}
		for _, node := range nodes.Items {
			checkImageStorage(&node)
		}
		time.Sleep(10 * time.Second)
	}
}

func checkImageStorage(node *api.Node) {
	var storage int64
	for _, image := range node.Status.Images {
		storage = storage + image.SizeBytes
	}
	changed := true
	if _, ok := imageCapacity[node.Name]; ok {
		if imageCapacity[node.Name] == storage {
			changed = false
		}
	}
	if changed {
		logrus.Infof("Node [%s] storage occupied by images changed. Old value: [%v], new value: [%v]", node.Name,
			humanize.Bytes(uint64(imageCapacity[node.Name])), humanize.Bytes(uint64(storage)))
		imageCapacity[node.Name] = storage
	} else {
		logrus.Infof("No changes in node [%s] storage occupied by images", node.Name)
	}
}

func getClient(pathToCfg string) (*kubernetes.Clientset, error) {
	var config *rest.Config
	var err error
	if pathToCfg == "" {
		logrus.Info("Using in cluster config")
		config, err = rest.InClusterConfig()
		// in cluster access
	} else {
		logrus.Info("Using out of cluster config")
		config, err = clientcmd.BuildConfigFromFlags("", pathToCfg)
	}
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}
