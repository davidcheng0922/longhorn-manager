package controller

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newInstanceManagerUpgrade(name, nodeID, targetImage string, state longhorn.InstanceManagerUpgradeState) *longhorn.InstanceManagerUpgrade {
	return &longhorn.InstanceManagerUpgrade{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: longhorn.InstanceManagerUpgradeSpec{
			NodeID:      nodeID,
			TargetImage: targetImage,
		},
		Status: longhorn.InstanceManagerUpgradeStatus{
			State: state,
		},
	}
}

func newTestInstanceManagerUpgradeController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*InstanceManagerUpgradeController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	imuc, err := NewInstanceManagerUpgradeController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	if err != nil {
		return nil, err
	}
	imuc.eventRecorder = record.NewFakeRecorder(100)
	for i := range imuc.cacheSyncs {
		imuc.cacheSyncs[i] = alwaysReady
	}

	return imuc, nil
}

func (s *TestSuite) TestGetNodeV2InstanceManager(c *C) {
	var err error
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	nodeIM := newInstanceManager("im-node", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	otherNodeIM := newInstanceManager("im-other-node", longhorn.InstanceManagerStateRunning, TestNode2, TestNode2, TestIP2, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	nodePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, nodeIM.Name, TestNamespace, TestNode1)
	otherNodePod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, otherNodeIM.Name, TestNamespace, TestNode2)

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactories.Start(stopCh)

	for _, im := range []*longhorn.InstanceManager{nodeIM, otherNodeIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
	}
	for _, pod := range []*corev1.Pod{nodePod, otherNodePod} {
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
	}
	c.Assert(cache.WaitForCacheSync(stopCh, ds.InstanceManagerInformer.HasSynced), Equals, true)

	selected, err := ds.GetNodeV2InstanceManagerRO(TestNode1)
	c.Assert(err, IsNil)
	c.Assert(selected, NotNil)
	c.Assert(selected.Name, Equals, nodeIM.Name)
	_ = imIndexer
}

func (s *TestSuite) TestGetNodeV2InstanceManagerPrefersRunningSourceIM(c *C) {
	var err error
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 30*time.Second)

	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	newDefaultIM := newInstanceManager("im-new-default", longhorn.InstanceManagerStateUpgrading, TestNode1, TestNode1, "", nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)

	stopCh := make(chan struct{})
	defer close(stopCh)
	informerFactories.Start(stopCh)

	for _, im := range []*longhorn.InstanceManager{sourceIM, newDefaultIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
	}
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), sourcePod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(sourcePod)
	c.Assert(err, IsNil)
	c.Assert(cache.WaitForCacheSync(stopCh, ds.InstanceManagerInformer.HasSynced), Equals, true)

	selected, err := ds.GetNodeV2InstanceManagerRO(TestNode1)
	c.Assert(err, IsNil)
	c.Assert(selected, NotNil)
	c.Assert(selected.Name, Equals, sourceIM.Name)
}

func (s *TestSuite) TestEnsureSourceIMUpgradeTriggered(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateRelocatingEngines)
	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, TestNamespace, TestNode1)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	err = imuc.ensureSourceIMUpgradeTriggered(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)

	updated, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Get(context.TODO(), im.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updated.Spec.Image, Equals, TestExtraInstanceManagerImage)
}

func (s *TestSuite) TestWaitingForSourceIMWaitsForTargetPodSpecImage(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	tempIM := newInstanceManager("im-temp", longhorn.InstanceManagerStateRunning, TestNode2, TestNode2, TestIP2, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)
	sourcePod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}
	sourcePod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Image: TestInstanceManagerImage, Ready: true}}
	tempPod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, tempIM.Name, TestNamespace, TestNode2)
	tempPod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}
	tempPod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Image: TestInstanceManagerImage, Ready: true}}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.EngineNodeID = TestNode2
	volume.Status.CurrentEngineNodeID = TestNode2

	for _, im := range []*longhorn.InstanceManager{sourceIM, tempIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}
	for _, pod := range []*corev1.Pod{sourcePod, tempPod} {
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
	}
	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForSourceIM(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
}

func (s *TestSuite) TestWaitingForSourceIMAdvancesAfterTargetPodSpecImageAndSPDKReady(c *C) {
	origEnsureSPDKTargetIsReady := ensureSPDKTargetIsReady
	defer func() {
		ensureSPDKTargetIsReady = origEnsureSPDKTargetIsReady
	}()
	ensureSPDKTargetIsReady = func(_ *InstanceManagerUpgradeController, _ *longhorn.InstanceManager, _ *logrus.Entry) (bool, error) {
		return true, nil
	}

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	tempIM := newInstanceManager("im-temp", longhorn.InstanceManagerStateRunning, TestNode2, TestNode2, TestIP2, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)
	sourcePod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestExtraInstanceManagerImage}}
	tempPod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, tempIM.Name, TestNamespace, TestNode2)
	tempPod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}
	tempPod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Image: TestInstanceManagerImage, Ready: true}}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.EngineNodeID = TestNode2
	volume.Status.CurrentEngineNodeID = TestNode2

	for _, im := range []*longhorn.InstanceManager{sourceIM, tempIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}
	for _, pod := range []*corev1.Pod{sourcePod, tempPod} {
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
	}
	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForSourceIM(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateRestoringEngines)
}

func (s *TestSuite) TestWaitingForSourceIMWaitsWhenSPDKTargetIsNotReady(c *C) {
	origEnsureSPDKTargetIsReady := ensureSPDKTargetIsReady
	defer func() {
		ensureSPDKTargetIsReady = origEnsureSPDKTargetIsReady
	}()
	ensureSPDKTargetIsReady = func(_ *InstanceManagerUpgradeController, _ *longhorn.InstanceManager, _ *logrus.Entry) (bool, error) {
		return false, nil
	}

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	sourceIM := newInstanceManager("im-source", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	tempIM := newInstanceManager("im-temp", longhorn.InstanceManagerStateRunning, TestNode2, TestNode2, TestIP2, nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
	sourcePod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, sourceIM.Name, TestNamespace, TestNode1)
	sourcePod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestExtraInstanceManagerImage}}
	tempPod := newPod(&corev1.PodStatus{PodIP: TestIP2, Phase: corev1.PodRunning}, tempIM.Name, TestNamespace, TestNode2)
	tempPod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}
	tempPod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Image: TestInstanceManagerImage, Ready: true}}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.EngineNodeID = TestNode2
	volume.Status.CurrentEngineNodeID = TestNode2

	for _, im := range []*longhorn.InstanceManager{sourceIM, tempIM} {
		_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = imIndexer.Add(im)
		c.Assert(err, IsNil)
	}
	for _, pod := range []*corev1.Pod{sourcePod, tempPod} {
		_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(pod)
		c.Assert(err, IsNil)
	}
	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForSourceIM(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
}

func (s *TestSuite) TestWaitingForHealthyVolumesWaitsForOriginalNodeHealthBeforeComplete(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Status.CurrentEngineNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessDegraded

	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForHealthyVolumes(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
}

func (s *TestSuite) TestWaitingForHealthyVolumesCompletesAfterOriginalNodeHealthy(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Status.CurrentEngineNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessHealthy

	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileWaitingForHealthyVolumes(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateCompleted)
}

func (s *TestSuite) TestRestoringEnginesWaitsForOriginalNodeSwitchover(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Spec.EngineNodeID = TestNode2
	volume.Status.CurrentEngineNodeID = TestNode2
	volume.Status.Robustness = longhorn.VolumeRobustnessDegraded

	_, err = lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileRestoringEngines(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateRestoringEngines)
}

func (s *TestSuite) TestRestoringEnginesWaitsForHealthyVolumesAfterOriginalNodeSwitchover(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	imuc, err := newTestInstanceManagerUpgradeController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateRestoringEngines)
	imu.Status.Engines = map[string]longhorn.EngineRelocation{
		TestVolumeName: {
			OriginalNodeID:  TestNode1,
			TemporaryNodeID: TestNode2,
		},
	}
	volume := newVolume(TestVolumeName, 2)
	volume.Namespace = TestNamespace
	volume.Status.CurrentEngineNodeID = TestNode1
	volume.Status.Robustness = longhorn.VolumeRobustnessDegraded

	err = volumeIndexer.Add(volume)
	c.Assert(err, IsNil)

	err = imuc.reconcileRestoringEngines(imu, logrus.NewEntry(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	c.Assert(imu.Status.State, Equals, longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes)
}

func (s *TestSuite) TestSyncInPlaceUpgradedInstanceManagerPod(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
	pod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestInstanceManagerImage}}

	for _, obj := range []cache.Indexer{imIndexer, imuIndexer, pIndexer} {
		_ = obj
	}

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	handled, err := imc.syncInPlaceUpgradedInstanceManagerPod(im, imu)
	c.Assert(err, IsNil)
	c.Assert(handled, Equals, true)

	updatedPod, err := kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(updatedPod.Spec.Containers[0].Image, Equals, TestExtraInstanceManagerImage)
}

func (s *TestSuite) TestSyncInPlaceUpgradedInstanceManagerPodDoesNotBlockRecreateWhenPodMissing(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	handled, err := imc.syncInPlaceUpgradedInstanceManagerPod(im, imu)
	c.Assert(err, IsNil)
	c.Assert(handled, Equals, false)
}

func (s *TestSuite) TestSyncStatusWithPodSetsUpgradingDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
	pod.Spec.Containers = []corev1.Container{{Name: "instance-manager", Image: TestExtraInstanceManagerImage}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "instance-manager", Ready: false}}

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.CurrentState, Equals, longhorn.InstanceManagerStateUpgrading)
	c.Assert(types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypePodReady).Reason, Equals, longhorn.InstanceManagerConditionReasonPodUpgrading)
}

func (s *TestSuite) TestSyncStatusWithPodSetsUpgradingWhenPodMissingDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.CurrentState, Equals, longhorn.InstanceManagerStateUpgrading)
	c.Assert(types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypePodReady).Reason, Equals, longhorn.InstanceManagerConditionReasonPodUpgrading)
}

func (s *TestSuite) TestHandlePodRecreatesMissingPodDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	lhNodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()
	kubeNodeIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	createDangerZoneSettingsForV2(c, lhClient, sIndexer)

	kubeNode := newKubernetesNode(TestNode1, corev1.ConditionTrue, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
	_, err = kubeClient.CoreV1().Nodes().Create(context.TODO(), kubeNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = kubeNodeIndexer.Add(kubeNode)
	c.Assert(err, IsNil)

	lhNode := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	_, err = lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), lhNode, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = lhNodeIndexer.Add(lhNode)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateUpgrading, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	err = imc.handlePod(im)
	c.Assert(err, IsNil)

	pod, err := kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), im.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(pod.Spec.Containers[0].Name, Equals, "instance-manager")
}

func (s *TestSuite) TestSyncStatusWithPodSetsUpgradingWhenPodDeletingDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	pod := newPod(&corev1.PodStatus{PodIP: TestIP1, Phase: corev1.PodRunning}, im.Name, im.Namespace, im.Spec.NodeID)
	now := metav1.Now()
	pod.DeletionTimestamp = &now

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.CurrentState, Equals, longhorn.InstanceManagerStateUpgrading)
	c.Assert(types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypePodReady).Reason, Equals, longhorn.InstanceManagerConditionReasonPodUpgrading)
}

func (s *TestSuite) TestSyncStatusWithPodSetsUpgradingWhenPodFailedDuringLiveUpgrade(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
	imuIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagerUpgrades().Informer().GetIndexer()
	pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateRunning, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	imu := newInstanceManagerUpgrade("imu-test", TestNode1, TestExtraInstanceManagerImage, longhorn.InstanceManagerUpgradeStateWaitingForSourceIM)
	pod := newPod(&corev1.PodStatus{Phase: corev1.PodFailed}, im.Name, im.Namespace, im.Spec.NodeID)

	_, err = lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imIndexer.Add(im)
	c.Assert(err, IsNil)

	_, err = lhClient.LonghornV1beta2().InstanceManagerUpgrades(TestNamespace).Create(context.TODO(), imu, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = imuIndexer.Add(imu)
	c.Assert(err, IsNil)

	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)

	err = imc.syncStatusWithPod(im)
	c.Assert(err, IsNil)
	c.Assert(im.Status.CurrentState, Equals, longhorn.InstanceManagerStateUpgrading)
	c.Assert(types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypePodReady).Reason, Equals, longhorn.InstanceManagerConditionReasonPodUpgrading)
}

func (s *TestSuite) TestAreDangerZoneSettingsSyncedToIMPodShortCircuitsForUpgrading(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	imc, err := newTestInstanceManagerController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	im := newInstanceManager("im-upgrade", longhorn.InstanceManagerStateUpgrading, TestNode1, TestNode1, TestIP1, nil, nil, nil, longhorn.DataEngineTypeV2, TestExtraInstanceManagerImage, false)
	isSynced, unsynced, isPodDeletedOrNotRunning, areInstancesRunningInPod, err := imc.areDangerZoneSettingsSyncedToIMPod(im)
	c.Assert(err, IsNil)
	c.Assert(isSynced, Equals, true)
	c.Assert(len(unsynced), Equals, 0)
	c.Assert(isPodDeletedOrNotRunning, Equals, true)
	c.Assert(areInstancesRunningInPod, Equals, false)
}
