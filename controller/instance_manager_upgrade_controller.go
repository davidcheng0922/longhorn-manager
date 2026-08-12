package controller

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	// instanceManagerUpgradeRequeueAfter is the duration after which an active upgrade is re-enqueued for processing.
	// This controls how frequently the controller checks for upgrade timeouts and temp node health during the upgrade process.
	instanceManagerUpgradeRequeueAfter = 10 * time.Second
)

var (
	sourceIMSPDKTargetReadyProbeDuration = time.Minute
	sourceIMSPDKTargetReadyProbeInterval = 2 * time.Second
	ensureSPDKTargetIsReady              = (*InstanceManagerUpgradeController).ensureSPDKTargetIsReady
)

// errUpgradePrecondition is returned by buildEngineRelocationPlan when the
// upgrade cannot proceed due to a recoverable precondition (e.g. degraded
// volume, no healthy replica on another node). Callers that want to wait
// rather than propagate should check with errors.Is(err, errUpgradePrecondition).
var errUpgradePrecondition = errors.New("upgrade precondition not met")
var errUpgradeUnsupported = errors.New("upgrade requirement unsupported")

type InstanceManagerUpgradeController struct {
	*baseController

	namespace    string
	controllerID string

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder
}

func NewInstanceManagerUpgradeController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace string,
	controllerID string,
) (*InstanceManagerUpgradeController, error) {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	imuc := &InstanceManagerUpgradeController{
		baseController: newBaseController("longhorn-instance-manager-upgrade", logger),

		ds:           ds,
		namespace:    namespace,
		controllerID: controllerID,
		kubeClient:   kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(
			scheme,
			corev1.EventSource{Component: "longhorn-instance-manager-upgrade-controller"},
		),
	}

	var err error

	if _, err = ds.InstanceManagerUpgradeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: imuc.enqueueInstanceManagerUpgrade,
		UpdateFunc: func(oldObj, newObj interface{}) {
			imuc.enqueueInstanceManagerUpgrade(newObj)
		},
		DeleteFunc: imuc.enqueueInstanceManagerUpgrade,
	}); err != nil {
		return nil, err
	}
	imuc.cacheSyncs = append(imuc.cacheSyncs, ds.InstanceManagerUpgradeInformer.HasSynced)

	if _, err = ds.InstanceManagerInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    imuc.enqueueInstanceManagerChange,
		UpdateFunc: func(oldObj, newObj interface{}) { imuc.enqueueInstanceManagerChange(newObj) },
		DeleteFunc: imuc.enqueueInstanceManagerChange,
	}); err != nil {
		return nil, err
	}
	imuc.cacheSyncs = append(imuc.cacheSyncs, ds.InstanceManagerInformer.HasSynced)

	if _, err = ds.VolumeInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    imuc.enqueueVolumeChange,
		UpdateFunc: func(oldObj, newObj interface{}) { imuc.enqueueVolumeChange(newObj) },
		DeleteFunc: imuc.enqueueVolumeChange,
	}); err != nil {
		return nil, err
	}
	imuc.cacheSyncs = append(imuc.cacheSyncs, ds.VolumeInformer.HasSynced)

	if _, err = ds.EngineInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    imuc.enqueueEngineChange,
		UpdateFunc: func(oldObj, newObj interface{}) { imuc.enqueueEngineChange(newObj) },
		DeleteFunc: imuc.enqueueEngineChange,
	}); err != nil {
		return nil, err
	}
	imuc.cacheSyncs = append(imuc.cacheSyncs, ds.EngineInformer.HasSynced)

	return imuc, nil
}

func (imuc *InstanceManagerUpgradeController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer imuc.queue.ShutDown()

	imuc.logger.Info("Starting Longhorn instance manager upgrade controller")
	defer imuc.logger.Info("Shut down Longhorn instance manager upgrade controller")

	if !cache.WaitForNamedCacheSync("longhorn instance manager upgrades", stopCh, imuc.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(imuc.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (imuc *InstanceManagerUpgradeController) worker() {
	for imuc.processNextWorkItem() {
	}
}

func (imuc *InstanceManagerUpgradeController) processNextWorkItem() bool {
	key, quit := imuc.queue.Get()
	if quit {
		return false
	}
	defer imuc.queue.Done(key)

	err := imuc.syncInstanceManagerUpgrade(key.(string))
	imuc.handleErr(err, key)

	return true
}

func (imuc *InstanceManagerUpgradeController) handleErr(err error, key interface{}) {
	if err == nil {
		imuc.queue.Forget(key)
		return
	}

	log := imuc.logger.WithField("instanceManagerUpgrade", key)
	if imuc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn instance manager upgrade")
		imuc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn instance manager upgrade out of the queue")
	imuc.queue.Forget(key)
}

func getLoggerForInstanceManagerUpgrade(logger logrus.FieldLogger, imu *longhorn.InstanceManagerUpgrade) *logrus.Entry {
	return logger.WithFields(logrus.Fields{"instanceManagerUpgrade": imu.Name, "node": imu.Spec.NodeID})
}

func (imuc *InstanceManagerUpgradeController) isResponsibleFor(imu *longhorn.InstanceManagerUpgrade) bool {
	return isControllerResponsibleFor(imuc.controllerID, imuc.ds, imu.Name, imu.Spec.NodeID, imu.Status.OwnerID)
}

func (imuc *InstanceManagerUpgradeController) enqueueInstanceManagerUpgrade(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}
	imuc.queue.Add(key)
}

func (imuc *InstanceManagerUpgradeController) enqueueInstanceManagerChange(obj interface{}) {
	im, ok := obj.(*longhorn.InstanceManager)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		im, ok = deletedState.Obj.(*longhorn.InstanceManager)
		if !ok {
			return
		}
	}

	imus, err := imuc.ds.ListInstanceManagerUpgradesRO()
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, imu := range imus {
		if imu.Spec.NodeID == im.Spec.NodeID {
			imuc.enqueueInstanceManagerUpgrade(imu)
			continue
		}

		// Re-evaluate Pending IMUs when any v2 AllInOne IM changes, since a
		// newly Running IM on another node may now satisfy the temporary-node
		// preconditions. Also watch IMs on already planned temporary nodes so
		// relocation/readiness can progress promptly without waiting for an
		// unrelated IMU/volume event to requeue the upgrade.
		if im.Spec.Type == longhorn.InstanceManagerTypeAllInOne &&
			types.IsDataEngineV2(im.Spec.DataEngine) &&
			imu.Status.State == longhorn.InstanceManagerUpgradeStatePending {
			imuc.enqueueInstanceManagerUpgrade(imu)
			continue
		}

		for _, reloc := range imu.Status.Engines {
			if reloc.TemporaryNodeID == im.Spec.NodeID {
				imuc.enqueueInstanceManagerUpgrade(imu)
				break
			}
		}
	}
}

func (imuc *InstanceManagerUpgradeController) enqueueVolumeChange(obj interface{}) {
	volume, ok := obj.(*longhorn.Volume)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		volume, ok = deletedState.Obj.(*longhorn.Volume)
		if !ok {
			return
		}
	}

	imus, err := imuc.ds.ListInstanceManagerUpgradesRO()
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	for _, imu := range imus {
		if _, ok := imu.Status.Engines[volume.Name]; ok {
			imuc.enqueueInstanceManagerUpgrade(imu)
		}
	}
}

func (imuc *InstanceManagerUpgradeController) enqueueEngineChange(obj interface{}) {
	engine, ok := obj.(*longhorn.Engine)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		engine, ok = deletedState.Obj.(*longhorn.Engine)
		if !ok {
			return
		}
	}

	imus, err := imuc.ds.ListInstanceManagerUpgradesRO()
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	for _, imu := range imus {
		// Enqueue the IMU if this engine belongs to the upgrade's source node
		// or appears in the relocation plan (temporary node case).
		if imu.Spec.NodeID == engine.Spec.NodeID {
			imuc.enqueueInstanceManagerUpgrade(imu)
			continue
		}
		if _, ok := imu.Status.Engines[engine.Spec.VolumeName]; ok {
			imuc.enqueueInstanceManagerUpgrade(imu)
		}
	}
}

func (imuc *InstanceManagerUpgradeController) syncInstanceManagerUpgrade(key string) (err error) {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != imuc.namespace {
		return nil
	}

	imu, err := imuc.ds.GetInstanceManagerUpgrade(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return err
	}
	if imu == nil {
		return nil
	}

	log := getLoggerForInstanceManagerUpgrade(imuc.logger, imu)

	if !imuc.isResponsibleFor(imu) {
		return nil
	}

	if imu.Status.OwnerID != imuc.controllerID {
		imu.Status.OwnerID = imuc.controllerID
		imu, err = imuc.ds.UpdateInstanceManagerUpgradeStatus(imu)
		if err != nil {
			return err
		}
		log.Infof("InstanceManagerUpgrade got new owner %v", imuc.controllerID)
	}

	imu = imu.DeepCopy()
	existingStatus := imu.Status.DeepCopy()

	defer func() {
		if err == nil && !reflect.DeepEqual(existingStatus, &imu.Status) {
			if _, updateErr := imuc.ds.UpdateInstanceManagerUpgradeStatus(imu); updateErr != nil {
				err = updateErr
			}
		}
	}()

	if imu.DeletionTimestamp != nil {
		imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonDelete, "Deleting instance manager upgrade %v", imu.Name)
		return imuc.ds.RemoveFinalizerForInstanceManagerUpgrade(imu)
	}

	err = imuc.reconcileStateMachine(imu, log)
	if err != nil {
		return err
	}

	// Re-enqueue periodically to renew the lease and enforce the upgrade timeout
	// while the upgrade is in a transient state.
	if imu.Status.State == longhorn.InstanceManagerUpgradeStatePending ||
		types.IsActiveInstanceManagerUpgradeState(imu.Status.State) {
		imuc.queue.AddAfter(key, instanceManagerUpgradeRequeueAfter)
	}

	return nil
}

func (imuc *InstanceManagerUpgradeController) reconcileStateMachine(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	if shouldStop, err := imuc.enforceUpgradeTimeout(imu, log); err != nil {
		return err
	} else if shouldStop {
		return nil
	}

	switch imu.Status.State {
	case "":
		imu.Status.State = longhorn.InstanceManagerUpgradeStatePending
		fallthrough

	case longhorn.InstanceManagerUpgradeStatePending:
		if imu.Status.AbortRequested {
			imuc.markAborted(imu, log,
				"Abort requested (%s) while pending, marking upgrade as failed",
				longhorn.InstanceManagerUpgradeStateFailed)
			return nil
		}
		return imuc.reconcilePending(imu, log)

	case longhorn.InstanceManagerUpgradeStateRelocatingEngines:
		// If an abort is requested, skip relocation and begin restoring engines.
		// Note: We no longer reset StartedAt here. The single global timeout applies
		// to the entire upgrade lifecycle, providing predictable timeout behavior.
		if imu.Status.AbortRequested {
			imuc.markAborted(imu, log,
				"Abort requested (%s), transitioning to restore engines to original positions",
				longhorn.InstanceManagerUpgradeStateRestoringEngines)
			return nil
		}
		return imuc.reconcileRelocatingEngines(imu, log)

	case longhorn.InstanceManagerUpgradeStateWaitingForSourceIM:
		// If an abort is requested, begin restoring engines.
		// Note: We no longer reset StartedAt here. The single global timeout applies
		// to the entire upgrade lifecycle, providing predictable timeout behavior.
		if imu.Status.AbortRequested {
			imuc.markAborted(imu, log,
				"Abort requested (%s) while waiting for source IM, transitioning to restore engines",
				longhorn.InstanceManagerUpgradeStateRestoringEngines)
			return nil
		}
		return imuc.reconcileWaitingForSourceIM(imu, log)

	case longhorn.InstanceManagerUpgradeStateRestoringEngines:
		return imuc.reconcileRestoringEngines(imu, log)

	case longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes:
		return imuc.reconcileWaitingForHealthyVolumes(imu, log)

	case longhorn.InstanceManagerUpgradeStateCompleted, longhorn.InstanceManagerUpgradeStateFailed:
		return nil

	default:
		return fmt.Errorf("unknown instance manager upgrade state %v", imu.Status.State)
	}
}

func (imuc *InstanceManagerUpgradeController) getIMUAbortReason(imu *longhorn.InstanceManagerUpgrade) string {
	if imu.Status.AbortReason != "" {
		return imu.Status.AbortReason
	}
	return "aborted"
}

func (imuc *InstanceManagerUpgradeController) markAborted(
	imu *longhorn.InstanceManagerUpgrade,
	log *logrus.Entry,
	message string,
	nextState longhorn.InstanceManagerUpgradeState,
) {
	reason := imuc.getIMUAbortReason(imu)
	log.Infof(message, reason)
	imu.Status.State = nextState
	if imu.Status.ErrorMsg == "" {
		imu.Status.ErrorMsg = fmt.Sprintf("upgrade aborted: %s", reason)
	}
}

func (imuc *InstanceManagerUpgradeController) enforceUpgradeTimeout(
	imu *longhorn.InstanceManagerUpgrade,
	log *logrus.Entry,
) (bool, error) {
	if (imu.Status.State != longhorn.InstanceManagerUpgradeStatePending &&
		!types.IsActiveInstanceManagerUpgradeState(imu.Status.State)) || imu.Status.StartedAt == "" {
		return false, nil
	}

	// If already aborting in RestoringEngines state, skip timeout enforcement
	// to allow the restore process to complete. Otherwise timeout would block
	// restore progress indefinitely.
	if imu.Status.State == longhorn.InstanceManagerUpgradeStateRestoringEngines && imu.Status.AbortRequested {
		return false, nil
	}

	startedAt, err := util.ParseTime(imu.Status.StartedAt)
	if err != nil {
		log.WithError(err).Warnf("Failed to parse StartedAt %v for IMU %v", imu.Status.StartedAt, imu.Name)
		return false, nil
	}

	// Read the timeout setting (in minutes)
	timeoutMinutes, err := imuc.ds.GetSettingAsInt(types.SettingNameV2InstanceManagerUpgradeTimeout)
	if err != nil {
		log.WithError(err).Warnf("Failed to get %v setting, using default 60 minutes", types.SettingNameV2InstanceManagerUpgradeTimeout)
		timeoutMinutes = 60
	}
	upgradeTimeout := time.Duration(timeoutMinutes) * time.Minute

	if time.Since(startedAt) <= upgradeTimeout {
		return false, nil
	}

	// Timeout detected: transition to RestoringEngines to revert engines back to original nodes.
	// We use a single global timeout - StartedAt is never reset, so this provides predictable
	// timeout behavior regardless of which state the upgrade is stuck in.
	log.Warnf("IMU %v timed out in state %v after %v, transitioning to restore engines", imu.Name, imu.Status.State, upgradeTimeout)
	imu.Status.AbortRequested = true
	imu.Status.AbortReason = "timeout"
	imu.Status.State = longhorn.InstanceManagerUpgradeStateRestoringEngines
	imu.Status.ErrorMsg = "upgrade timed out; reverting engines to original nodes"
	return true, nil
}

// reconcilePending validates the upgrade request, finds the source instance
// manager on the node, initializes the engine relocation plan, and transitions
// to the appropriate next state.
func (imuc *InstanceManagerUpgradeController) reconcilePending(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	// Validate required spec fields.
	if imu.Spec.TargetImage == "" || imu.Spec.NodeID == "" {
		imu.Status.ErrorMsg = "missing required spec fields"
		imu.Status.State = longhorn.InstanceManagerUpgradeStateFailed
		return nil
	}

	// Find the single v2 AllInOne IM on this node that participates in the in-place upgrade.
	nodeIM, err := imuc.ds.GetNodeV2InstanceManagerRO(imu.Spec.NodeID)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			nodeIM = nil
		} else {
			return err
		}
	}

	if nodeIM == nil {
		// No IM found; check if the node already converged on the target image.
		converged, err := imuc.isUpgradeAlreadyConverged(imu)
		if err != nil {
			return err
		}
		if converged {
			log.Infof("Node %v already running target image, upgrade converged", imu.Spec.NodeID)
			imu.Status.State = longhorn.InstanceManagerUpgradeStateCompleted
		} else {
			log.Infof("Upgrade IM not found on node %v, waiting", imu.Spec.NodeID)
			imuc.markStartedAt(imu)
			imu.Status.State = longhorn.InstanceManagerUpgradeStateWaitingForSourceIM
		}
		return nil
	}

	// If the node IM is already running the target image, we're done.
	if nodeIM.Spec.Image == imu.Spec.TargetImage && nodeIM.Status.CurrentState == longhorn.InstanceManagerStateRunning {
		log.Infof("Node IM %v already running target image, upgrade converged", nodeIM.Name)
		imu.Status.State = longhorn.InstanceManagerUpgradeStateCompleted
		return nil
	}

	// The node IM exists but is not yet Running (node down, IM restarting, etc.).
	// Engines are not running either, so we cannot relocate them yet.
	// Wait here in Pending — when the IM recovers we will either find it
	// running the target image (converged) or running the old image and
	// proceed with relocation normally.
	if nodeIM.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		log.Debugf("Node IM %v is not running (state: %v), waiting before building relocation plan",
			nodeIM.Name, nodeIM.Status.CurrentState)
		imuc.markStartedAt(imu)
		return nil
	}

	plan, err := imuc.buildEngineRelocationPlan(imu)
	if err != nil {
		if errors.Is(err, errUpgradeUnsupported) {
			imu.Status.ErrorMsg = err.Error()
			imu.Status.State = longhorn.InstanceManagerUpgradeStateFailed
			return nil
		}
		if errors.Is(err, errUpgradePrecondition) {
			// Recoverable: wait for preconditions (e.g. degraded volume rebuilding, no temp node yet).
			log.WithError(err).Debug("Engine relocation plan blocked by precondition, waiting")
			imuc.markStartedAt(imu)
			return nil
		}
		return err
	}

	imu.Status.Engines = plan
	imuc.markStartedAt(imu)

	if len(plan) == 0 {
		if err := imuc.ensureSourceIMUpgradeTriggered(imu, log); err != nil {
			return err
		}
		log.Infof("No engines to relocate on node %v, waiting for source IM in-place upgrade to recover", imu.Spec.NodeID)
		imu.Status.State = longhorn.InstanceManagerUpgradeStateWaitingForSourceIM
		return nil
	}

	log.Infof("Engine relocation plan built with %d engine(s), beginning relocation", len(plan))
	imu.Status.State = longhorn.InstanceManagerUpgradeStateRelocatingEngines
	return nil
}

func (imuc *InstanceManagerUpgradeController) ensureSourceIMUpgradeTriggered(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	nodeIM, err := imuc.ds.GetNodeV2InstanceManagerRO(imu.Spec.NodeID)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return err
	}

	nodeIM = nodeIM.DeepCopy()
	if nodeIM.Spec.Image == imu.Spec.TargetImage {
		return nil
	}
	nodeIM.Spec.Image = imu.Spec.TargetImage
	if _, err := imuc.ds.UpdateInstanceManager(nodeIM); err != nil {
		return errors.Wrapf(err, "failed to update node IM %v image to %v for in-place upgrade", nodeIM.Name, imu.Spec.TargetImage)
	}

	log.Infof("Triggered in-place image upgrade for node IM %v to target image %v", nodeIM.Name, imu.Spec.TargetImage)
	imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
		"Triggered in-place image upgrade for node IM %v to target image %v", nodeIM.Name, imu.Spec.TargetImage)
	return nil
}

// reconcileRelocatingEngines moves v2 engines (NVMe-oF targets) away from the
// source node to their assigned temporary nodes. The EngineFrontend (NVMe-oF
// initiator, kernel-level) deliberately stays on the source node throughout —
// it survives the IM pod restart because it runs in the kernel.
// When all volumes have switched their current engine node to the temporary
// nodes, the state advances to WaitingForSourceIM.
func (imuc *InstanceManagerUpgradeController) reconcileRelocatingEngines(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	allRelocated := true

	for volumeName, reloc := range imu.Status.Engines {
		volume, err := imuc.ds.GetVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Volume %v not found during relocation, removing from plan", volumeName)
				imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonDelete,
					"Volume %v deleted during relocation, removing from upgrade plan", volumeName)
				delete(imu.Status.Engines, volumeName)
				continue
			}
			return err
		}

		// Already running on the temporary node — done for this engine.
		if volume.Status.CurrentEngineNodeID == reloc.TemporaryNodeID {
			continue
		}

		allRelocated = false

		// Volume has been directed to the temporary node but switchover is not yet complete.
		// Check whether the temporary node's IM is still healthy. If it is
		// down, re-select a new temporary node rather than waiting for timeout.
		if volume.Spec.EngineNodeID == reloc.TemporaryNodeID {
			abortUpgrade, newTempNode, err := imuc.handleTemporaryNodeFailureForVolume(imu, volumeName, reloc, volume, log)
			if err != nil {
				return err
			}
			if abortUpgrade {
				log.Warnf("Aborting upgrade after volume %v lost all temporary-node options", volumeName)
				imu.Status.AbortRequested = true
				imu.Status.AbortReason = "no-temporary-node"
				imu.Status.ErrorMsg = "upgrade aborted: no-temporary-node"
				imu.Status.State = longhorn.InstanceManagerUpgradeStateRestoringEngines
				return nil
			}
			if newTempNode != "" && volume.Spec.EngineNodeID != newTempNode {
				volume.Spec.EngineNodeID = newTempNode
				if _, err := imuc.ds.UpdateVolume(volume); err != nil {
					return errors.Wrapf(err, "failed to redirect volume %v to new temporary node %v", volumeName, newTempNode)
				}
			}
			// IM is up — switchover is still in progress; wait.
			continue
		}

		// Volume engine has not yet been directed to the temporary node.
		// Ensure a pre-upgrade snapshot (for fast replica rebuild) is ready
		// before relocating so that, if a replica is lost during the upgrade,
		// the rebuild can use the local snapshot rather than transferring the
		// full volume data.
		snapshotReady, err := imuc.ensurePreUpgradeSnapshot(volumeName, imu, log)
		if err != nil {
			return err
		}
		if !snapshotReady {
			continue
		}

		// The first source->temporary relocation requires the source IM to be
		// Running — the EngineFrontend (NVMe-oF initiator) uses it via gRPC to
		// suspend I/O, switch the target IP, and resume. If the source IM is
		// down we must wait: redirecting the engine spec would start the SPDK
		// target on the temp node but the EF could never reconnect to it,
		// leaving the volume in a worse state. This gate applies only to the
		// initial relocation away from the source node; temp->temp re-plans
		// after relocation are handled separately in WaitingForSourceIM.
		sourceIMReady, err := imuc.ds.CheckInstanceManagersReadiness(longhorn.DataEngineTypeV2, imu.Spec.NodeID)
		if err != nil {
			log.WithError(err).Warnf("Failed to check source IM readiness before relocating volume %v, waiting", volumeName)
			continue
		}
		if !sourceIMReady {
			log.Debugf("Source IM on node %v is not ready, waiting before relocating volume %v", imu.Spec.NodeID, volumeName)
			continue
		}

		tempIMReady, err := imuc.ds.CheckInstanceManagersReadiness(longhorn.DataEngineTypeV2, reloc.TemporaryNodeID)
		if err != nil {
			log.WithError(err).Warnf("Failed to check temporary IM readiness on node %v before relocating volume %v, waiting", reloc.TemporaryNodeID, volumeName)
			continue
		}
		if !tempIMReady {
			log.Debugf("Temporary IM on node %v is not ready, waiting before relocating volume %v", reloc.TemporaryNodeID, volumeName)
			continue
		}

		log.Infof("Relocating volume %v from node %v to temporary node %v", volumeName, reloc.OriginalNodeID, reloc.TemporaryNodeID)
		volume.Spec.EngineNodeID = reloc.TemporaryNodeID
		if _, err := imuc.ds.UpdateVolume(volume); err != nil {
			return errors.Wrapf(err, "failed to update volume %v for relocation", volumeName)
		}
		imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
			"Relocating volume %v to temporary node %v", volumeName, reloc.TemporaryNodeID)
	}

	if allRelocated {
		if err := imuc.ensureSourceIMUpgradeTriggered(imu, log); err != nil {
			return err
		}
		log.Infof("All engines relocated from node %v, waiting for source IM in-place upgrade to recover", imu.Spec.NodeID)
		imu.Status.State = longhorn.InstanceManagerUpgradeStateWaitingForSourceIM
	}

	return nil
}

// reconcileWaitingForSourceIM monitors temporary nodes after the source IM
// in-place upgrade is triggered. It waits until the active source IM pod is
// running the target image before allowing any switch-back preparation.
func (imuc *InstanceManagerUpgradeController) reconcileWaitingForSourceIM(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	// Check temp node health for each engine. If a temp node/IM has gone down,
	// re-plan the affected volume to a new healthy node.
	for volumeName, reloc := range imu.Status.Engines {
		volume, err := imuc.ds.GetVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Volume %v not found while waiting for source IM, removing from plan", volumeName)
				imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonDelete,
					"Volume %v deleted while waiting for source IM, removing from upgrade plan", volumeName)
				delete(imu.Status.Engines, volumeName)
				continue
			}
			return err
		}
		abortUpgrade, newTempNode, err := imuc.handleTemporaryNodeFailureForVolume(imu, volumeName, reloc, volume, log)
		if err != nil {
			return err
		}
		if abortUpgrade {
			log.Warnf("Aborting upgrade while waiting for source IM after volume %v lost all temporary-node options", volumeName)
			imu.Status.AbortRequested = true
			imu.Status.AbortReason = "no-temporary-node"
			imu.Status.ErrorMsg = "upgrade aborted: no-temporary-node"
			imu.Status.State = longhorn.InstanceManagerUpgradeStateRestoringEngines
			return nil
		}
		if newTempNode != "" && volume.Spec.EngineNodeID != newTempNode {
			log.Infof("Re-directing volume %v to new temporary node %v (was %v) while waiting for source IM upgrade",
				volumeName, newTempNode, reloc.TemporaryNodeID)
			volume.Spec.EngineNodeID = newTempNode
			if _, err := imuc.ds.UpdateVolume(volume); err != nil {
				return errors.Wrapf(err, "failed to redirect volume %v to new temporary node %v", volumeName, newTempNode)
			}
			imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
				"Redirecting volume %v to new temporary node %v during wait for source IM upgrade", volumeName, newTempNode)
		}
	}

	nodeIM, err := imuc.ds.GetNodeV2InstanceManagerRO(imu.Spec.NodeID)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			log.Debugf("Waiting for source IM on node %v to become active with target image %v", imu.Spec.NodeID, imu.Spec.TargetImage)
			return nil
		}
		return err
	}
	pod, err := imuc.ds.GetPodRO(imuc.namespace, nodeIM.Name)
	if err != nil {
		return err
	}
	if nodeIM.Status.CurrentState != longhorn.InstanceManagerStateRunning ||
		nodeIM.Spec.Image != imu.Spec.TargetImage ||
		getInstanceManagerPodImage(pod, "instance-manager") != imu.Spec.TargetImage {
		log.Debugf("Waiting for source IM %v on node %v to run target image %v: state=%v specImage=%v",
			nodeIM.Name, imu.Spec.NodeID, imu.Spec.TargetImage, nodeIM.Status.CurrentState, nodeIM.Spec.Image)
		return nil
	}

	spdkReady, err := ensureSPDKTargetIsReady(imuc, nodeIM, log)
	if err != nil {
		return err
	}
	if !spdkReady {
		log.Debugf("Waiting for source IM %v on node %v SPDK target to stay ready", nodeIM.Name, imu.Spec.NodeID)
		return nil
	}

	if len(imu.Status.Engines) == 0 {
		imu.Status.State = longhorn.InstanceManagerUpgradeStateCompleted
	} else {
		log.Infof("Source IM on node %v is ready, restoring engines to original nodes", imu.Spec.NodeID)
		imu.Status.State = longhorn.InstanceManagerUpgradeStateRestoringEngines
	}
	return nil
}

func (imuc *InstanceManagerUpgradeController) ensureSPDKTargetIsReady(im *longhorn.InstanceManager, log *logrus.Entry) (bool, error) {
	node, err := imuc.ds.GetNodeRO(im.Spec.NodeID)
	if err != nil {
		return false, err
	}

	deadline := time.Now().Add(sourceIMSPDKTargetReadyProbeDuration)
	allProbesSucceeded := true

	for {
		// Force the SPDK disk service connection to stay stable for the full
		// probe duration before proceeding. This is a conservative safety guard
		// for the in-place IM pod image replacement path.
		probeSucceeded := false

		diskService, err := engineapi.NewDiskServiceClient(im, imuc.logger)
		if err != nil {
			log.WithError(err).Debugf("Source IM %v disk service client is not ready", im.Name)
		} else {
			func() {
				defer diskService.Close()

				for diskName, diskSpec := range node.Spec.Disks {
					if diskSpec.Type != longhorn.DiskTypeBlock {
						continue
					}

					if _, err := diskService.DiskReplicaInstanceList(string(diskSpec.Type), diskName, string(diskSpec.DiskDriver)); err != nil {
						log.WithError(err).Debugf("Source IM %v disk service SPDK probe failed for node disk %v", im.Name, diskName)
						return
					}
					probeSucceeded = true
					return
				}
			}()
		}

		if len(node.Spec.Disks) == 0 {
			log.Debugf("Source IM %v node %v has no disk to probe", im.Name, node.Name)
		}
		if !probeSucceeded {
			allProbesSucceeded = false
		}

		if !time.Now().Before(deadline) {
			return allProbesSucceeded, nil
		}
		time.Sleep(sourceIMSPDKTargetReadyProbeInterval)
	}
}

// reconcileRestoringEngines moves v2 engines back to their original node after
// the source IM has been successfully upgraded. The EngineFrontend (kernel-
// level NVMe-oF initiator) has remained on the source node throughout and will
// reconnect to the engine once it is back there.
// Transitions to WaitingForHealthyVolumes after all volumes switch back.
func (imuc *InstanceManagerUpgradeController) reconcileRestoringEngines(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	allRestored := true

	for volumeName, reloc := range imu.Status.Engines {
		volume, err := imuc.ds.GetVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Volume %v not found during restore, removing from plan", volumeName)
				imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonDelete,
					"Volume %v deleted during engine restore, removing from upgrade plan", volumeName)
				delete(imu.Status.Engines, volumeName)
				continue
			}
			return err
		}

		// Done after the volume current engine node switches back. Health
		// convergence is checked in WaitingForHealthyVolumes.
		if volume.Status.CurrentEngineNodeID == reloc.OriginalNodeID {
			continue
		}

		allRestored = false

		// Kick off restoration by updating spec.EngineNodeID if not yet done.
		if volume.Spec.EngineNodeID != reloc.OriginalNodeID {
			log.Infof("Restoring volume %v from temporary node %v back to original node %v", volumeName, reloc.TemporaryNodeID, reloc.OriginalNodeID)
			volume.Spec.EngineNodeID = reloc.OriginalNodeID
			if _, err := imuc.ds.UpdateVolume(volume); err != nil {
				return errors.Wrapf(err, "failed to update volume %v for restoration", volumeName)
			}
			imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
				"Restoring volume %v back to original node %v", volumeName, reloc.OriginalNodeID)
		}
		// else: restoration is in progress, wait for the volume controller to complete it.
	}

	if allRestored {
		// Check if this restore is due to an abort (user or controller-requested)
		if imu.Status.AbortRequested {
			imuc.markAborted(imu, log,
				"All engines restored to original nodes after abort (%s), marking upgrade as failed",
				longhorn.InstanceManagerUpgradeStateFailed)
		} else {
			log.Infof("All engines restored to original nodes, waiting for volumes to become healthy")
			imu.Status.State = longhorn.InstanceManagerUpgradeStateWaitingForHealthyVolumes
		}
	}

	return nil
}

// reconcileWaitingForHealthyVolumes waits for restored volumes to become
// healthy on their original nodes before completing the upgrade.
func (imuc *InstanceManagerUpgradeController) reconcileWaitingForHealthyVolumes(imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) error {
	allHealthy := true

	for volumeName, reloc := range imu.Status.Engines {
		volume, err := imuc.ds.GetVolume(volumeName)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				log.Warnf("Volume %v not found while waiting for health, removing from plan", volumeName)
				delete(imu.Status.Engines, volumeName)
				continue
			}
			return err
		}

		if volume.Status.CurrentEngineNodeID != reloc.OriginalNodeID {
			log.Debugf("Volume %v engine not yet on original node %v (current: %v), waiting",
				volumeName, reloc.OriginalNodeID, volume.Status.CurrentEngineNodeID)
			allHealthy = false
			continue
		}

		if volume.Status.Robustness != longhorn.VolumeRobustnessHealthy {
			log.Debugf("Volume %v not yet healthy on node %v (robustness: %v), waiting",
				volumeName, reloc.OriginalNodeID, volume.Status.Robustness)
			allHealthy = false
			continue
		}
	}

	if !allHealthy {
		return nil
	}

	log.Infof("All restored volumes are healthy on original nodes, upgrade completed")
	imu.Status.State = longhorn.InstanceManagerUpgradeStateCompleted
	imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
		"Instance manager upgrade for node %v completed successfully", imu.Spec.NodeID)

	return nil
}

// isUpgradeAlreadyConverged returns true when the source node already has a
// running v2 AllInOne instance manager with the target image.
func (imuc *InstanceManagerUpgradeController) isUpgradeAlreadyConverged(imu *longhorn.InstanceManagerUpgrade) (bool, error) {
	im, err := imuc.ds.GetNodeV2InstanceManagerRO(imu.Spec.NodeID)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return false, nil
		}
		return false, err
	}
	if im.Status.CurrentState == longhorn.InstanceManagerStateRunning && im.Spec.Image == imu.Spec.TargetImage {
		return true, nil
	}
	return false, nil
}

// markStartedAt stamps the current time into StartedAt if it has not been set
// yet. This records when the upgrade first enters a timed wait or active
// execution phase so the timeout is measured from real progress/wait time
// rather than object creation time.
func (imuc *InstanceManagerUpgradeController) markStartedAt(imu *longhorn.InstanceManagerUpgrade) {
	if imu.Status.StartedAt == "" {
		imu.Status.StartedAt = util.Now()
	}
}

// ---------------------------------------------------------------------------
// Relocation plan helpers
// ---------------------------------------------------------------------------

// buildEngineRelocationPlan scans the source node for running v2 engines
// (NVMe-oF targets) and assigns each a temporary relocation node that:
//   - is not the source node,
//   - has a healthy running replica for the volume, and
//   - has a running v2 AllInOne instance manager.
//
// The EngineFrontend (NVMe-oF initiator, kernel-level) is intentionally not
// moved — it stays on the source node and survives the IM pod restart.
//
// Returns an error if any engine is not running (upgrade pre-conditions not
// met) or if no suitable temporary node can be found for any engine.
func (imuc *InstanceManagerUpgradeController) buildEngineRelocationPlan(imu *longhorn.InstanceManagerUpgrade) (map[string]longhorn.EngineRelocation, error) {
	plan := map[string]longhorn.EngineRelocation{}

	engines, err := imuc.ds.ListEnginesByNodeRO(imu.Spec.NodeID)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list engines")
	}

	for _, engine := range engines {
		if !types.IsDataEngineV2(engine.Spec.DataEngine) {
			continue
		}

		// Pre-condition: engine must be running before we can safely relocate
		// it. A non-running engine indicates a degraded volume that blocks the
		// upgrade.
		if engine.Status.CurrentState != longhorn.InstanceStateRunning {
			return nil, fmt.Errorf("%w: engine %v is not running (state: %v)",
				errUpgradePrecondition, engine.Name, engine.Status.CurrentState)
		}

		tempNode, err := imuc.selectTemporaryNode(imu.Spec.NodeID, engine.Spec.VolumeName, plan)
		if err != nil {
			errType := errUpgradePrecondition
			if errors.Is(err, errUpgradeUnsupported) {
				errType = errUpgradeUnsupported
			}
			return nil, fmt.Errorf("%w: cannot find temporary node for engine %v (volume %v): %v",
				errType, engine.Name, engine.Spec.VolumeName, err)
		}

		// Use volume.Name as the key since engine names change during safe v2 switchover migrations
		plan[engine.Spec.VolumeName] = longhorn.EngineRelocation{
			OriginalNodeID:  engine.Spec.NodeID,
			TemporaryNodeID: tempNode,
		}
	}

	return plan, nil
}

// handleTemporaryNodeFailureForVolume checks whether the temporary node assigned
// to a volume is still usable. If the temp node's IM is down, it selects a new
// healthy temp node and updates the relocation plan, returning the replacement
// node ID so the caller can decide when to redirect volume.Spec.EngineNodeID.
// If no new candidate is available, the volume is directed back to the
// original node and the caller should abort this IMU attempt so a later IMUC
// retry can restart from a clean state.
func (imuc *InstanceManagerUpgradeController) handleTemporaryNodeFailureForVolume(
	imu *longhorn.InstanceManagerUpgrade,
	volumeName string,
	reloc longhorn.EngineRelocation,
	volume *longhorn.Volume,
	log *logrus.Entry,
) (bool, string, error) {
	imReady, err := imuc.ds.CheckInstanceManagersReadiness(longhorn.DataEngineTypeV2, reloc.TemporaryNodeID)
	if err != nil {
		if datastore.ErrorIsNotFound(err) || types.ErrorIsNotFound(err) {
			log.WithError(err).Warnf("Temporary node %v no longer has a running IM for volume %v, re-evaluating relocation", reloc.TemporaryNodeID, volumeName)
		} else {
			log.WithError(err).Warnf("Failed to check IM readiness on temporary node %v for volume %v", reloc.TemporaryNodeID, volumeName)
			return false, "", nil
		}
	}
	if err == nil && imReady {
		return false, "", nil
	}

	newTempNode, err := imuc.selectTemporaryNode(imu.Spec.NodeID, volumeName, imu.Status.Engines)
	if err != nil || newTempNode == reloc.TemporaryNodeID {
		// No alternative temp node — return the volume to the original node and
		// abort this upgrade attempt rather than letting reconcile flip-flop
		// EngineNodeID or wait indefinitely for the source IM upgrade to finish.
		if volume.Spec.EngineNodeID != reloc.OriginalNodeID {
			log.WithError(err).Warnf("No new temporary node for volume %v, reverting to original node %v", volumeName, reloc.OriginalNodeID)
			volume.Spec.EngineNodeID = reloc.OriginalNodeID
			if _, updateErr := imuc.ds.UpdateVolume(volume); updateErr != nil {
				return false, "", errors.Wrapf(updateErr, "failed to revert volume %v to original node %v", volumeName, reloc.OriginalNodeID)
			}
			imuc.eventRecorder.Eventf(imu, corev1.EventTypeWarning, constant.EventReasonUpdate,
				"No temporary node available for volume %v, reverting to original node %v", volumeName, reloc.OriginalNodeID)
		}
		return true, "", nil
	}

	updatedReloc := reloc
	updatedReloc.TemporaryNodeID = newTempNode
	imu.Status.Engines[volumeName] = updatedReloc
	log.Infof("Re-planning volume %v: temp node %v is down, new temp node %v", volumeName, reloc.TemporaryNodeID, newTempNode)
	imuc.eventRecorder.Eventf(imu, corev1.EventTypeNormal, constant.EventReasonUpdate,
		"Re-planning volume %v to new temporary node %v", volumeName, newTempNode)
	return false, newTempNode, nil
}

// selectTemporaryNode picks a relocation target for one engine frontend. The
// chosen node must:
//  1. Have a healthy running replica for volumeName (so the frontend has local
//     or near-local replica access after the move), and
//  2. Have a running v2 AllInOne instance manager (so the frontend process can
//     actually be hosted there).
//
// Returns an error if no suitable node is found — in which case the live
// upgrade cannot proceed and the IMU transitions to Failed.
func (imuc *InstanceManagerUpgradeController) selectTemporaryNode(sourceNodeID, volumeName string, currentPlan map[string]longhorn.EngineRelocation) (string, error) {
	replicas, err := imuc.ds.ListVolumeReplicasRO(volumeName)
	if err != nil {
		return "", errors.Wrapf(err, "failed to list replicas for volume %v", volumeName)
	}

	replicaNodes := map[string]struct{}{}
	// Collect nodes that have a healthy and currently running replica, excluding
	// the source node. Historical replica health alone is not sufficient for
	// live upgrade safety if the replica process is no longer running.
	healthyReplicaNodes := map[string]struct{}{}
	for _, r := range replicas {
		if r.Spec.NodeID == sourceNodeID || r.Spec.NodeID == "" {
			continue
		}
		replicaNodes[r.Spec.NodeID] = struct{}{}
		if r.Status.CurrentState != longhorn.InstanceStateRunning {
			continue
		}
		if r.Spec.FailedAt != "" || r.Spec.HealthyAt == "" {
			continue
		}
		healthyReplicaNodes[r.Spec.NodeID] = struct{}{}
	}

	if len(replicaNodes) == 0 {
		return "", fmt.Errorf("%w: no replica exists on nodes other than source node %v for volume %v; single-replica or co-located volumes are not supported for live upgrade",
			errUpgradeUnsupported, sourceNodeID, volumeName)
	}

	if len(healthyReplicaNodes) == 0 {
		return "", fmt.Errorf("no healthy replica found on nodes other than source node %v for volume %v; "+
			"waiting for another replica node to become healthy", sourceNodeID, volumeName)
	}

	var candidates []string
	// Among those replica nodes, find one with a running v2 AllInOne IM.
	for nodeID := range healthyReplicaNodes {
		im, err := imuc.ds.GetRunningInstanceManagerByNodeRO(nodeID, longhorn.DataEngineTypeV2)
		if err != nil {
			if datastore.ErrorIsNotFound(err) || types.ErrorIsNotFound(err) {
				continue
			}
			return "", errors.Wrapf(err, "failed to get running v2 instance manager on candidate temporary node %v for volume %v", nodeID, volumeName)
		}
		if im != nil {
			candidates = append(candidates, nodeID)
		}
	}

	if len(candidates) == 0 {
		return "", fmt.Errorf("no temporary node with a running v2 IM found among healthy replica nodes for volume %v", volumeName)
	}

	return imuc.chooseBestTemporaryNode(candidates, currentPlan), nil
}

// chooseBestTemporaryNode selects the best node among the candidates based on the current usage
// (to balance the load evenly across all available temporary nodes).
func (imuc *InstanceManagerUpgradeController) chooseBestTemporaryNode(candidates []string, currentPlan map[string]longhorn.EngineRelocation) string {
	usage := make(map[string]int)
	for _, nodeID := range candidates {
		// Start with the number of engines currently running on this node
		// (which represents the number of attached volumes).
		engines, err := imuc.ds.ListEnginesByNodeRO(nodeID)
		if err != nil {
			imuc.logger.WithError(err).Warnf("Failed to list engines for node %v when choosing best temporary node, deprioritizing this node", nodeID)
			// Set a very high usage to deprioritize this node (rather than skipping it entirely,
			// which would cause issues if all candidates fail).
			usage[nodeID] = 1<<31 - 1 // MaxInt32
			continue
		}
		count := 0
		for _, e := range engines {
			if types.IsDataEngineV2(e.Spec.DataEngine) {
				count++
			}
		}
		usage[nodeID] = count
	}

	// Add the volumes that are already planned to be relocated to these nodes
	for _, reloc := range currentPlan {
		if _, ok := usage[reloc.TemporaryNodeID]; ok {
			usage[reloc.TemporaryNodeID]++
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		if usage[candidates[i]] == usage[candidates[j]] {
			// tie-breaker: sort by node ID to ensure determinism
			return candidates[i] < candidates[j]
		}
		return usage[candidates[i]] < usage[candidates[j]]
	})

	return candidates[0]
}

// ensurePreUpgradeSnapshot creates a pre-upgrade snapshot for the volume when
// both FastReplicaRebuildEnabled and TakeSnapshotBeforeV2DataEngineUpgrade
// settings are enabled and snapshot data integrity checking is enabled for the
// volume. It returns (true, nil) when the snapshot is ready or not required,
// and (false, nil) when the snapshot has been created but its checksum has not
// yet been computed. The caller should re-check on the next reconcile cycle.
func (imuc *InstanceManagerUpgradeController) ensurePreUpgradeSnapshot(volumeName string, imu *longhorn.InstanceManagerUpgrade, log *logrus.Entry) (bool, error) {
	engine, err := imuc.ds.GetVolumeCurrentEngine(volumeName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get current engine for volume %v", volumeName)
	}

	fastReplicaRebuild, err := imuc.ds.GetSettingAsBoolByDataEngine(types.SettingNameFastReplicaRebuildEnabled, engine.Spec.DataEngine)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get %v setting for data engine %v", types.SettingNameFastReplicaRebuildEnabled, engine.Spec.DataEngine)
	}
	if !fastReplicaRebuild {
		return true, nil
	}

	takeSnapshot, err := imuc.ds.GetSettingAsBoolByDataEngine(types.SettingNameTakeSnapshotBeforeV2DataEngineUpgrade, engine.Spec.DataEngine)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get %v setting for data engine %v", types.SettingNameTakeSnapshotBeforeV2DataEngineUpgrade, engine.Spec.DataEngine)
	}
	if !takeSnapshot {
		return true, nil
	}

	dataIntegrity, err := imuc.ds.GetVolumeSnapshotDataIntegrity(volumeName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get snapshot data integrity setting for volume %v", volumeName)
	}
	if dataIntegrity != longhorn.SnapshotDataIntegrityEnabled {
		return true, nil
	}

	reloc := imu.Status.Engines[volumeName]

	// If a snapshot was already created, check if its checksum is ready.
	if reloc.SnapshotName != "" {
		snapshotReady, err := imuc.isPreUpgradeSnapshotReady(volumeName, reloc.SnapshotName, log)
		if err == nil || !datastore.ErrorIsNotFound(err) {
			return snapshotReady, err
		}
		// Snapshot was deleted externally; clear the name so we recreate the
		// deterministic pre-upgrade snapshot below.
		reloc.SnapshotName = ""
		imu.Status.Engines[volumeName] = reloc
	}

	snapshotName := getPreUpgradeSnapshotName(imu.Name, volumeName)
	_, err = imuc.ds.GetSnapshot(snapshotName)
	if err != nil {
		if !datastore.ErrorIsNotFound(err) {
			return false, errors.Wrapf(err, "failed to get pre-upgrade snapshot %v for volume %v", snapshotName, volumeName)
		}

		snapshotCR := &longhorn.Snapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name: snapshotName,
			},
			Spec: longhorn.SnapshotSpec{
				Volume:         volumeName,
				CreateSnapshot: true,
			},
		}
		_, err = imuc.ds.CreateSnapshot(snapshotCR)
		if err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return false, errors.Wrapf(err, "failed to create pre-upgrade snapshot for volume %v", volumeName)
			}
			_, err = imuc.ds.GetSnapshot(snapshotName)
			if err != nil && !datastore.ErrorIsNotFound(err) {
				return false, errors.Wrapf(err, "failed to get existing pre-upgrade snapshot %v for volume %v", snapshotName, volumeName)
			}
		}
		log.Infof("Created pre-upgrade snapshot %v for volume %v", snapshotName, volumeName)
	}

	reloc.SnapshotName = snapshotName
	imu.Status.Engines[volumeName] = reloc
	return imuc.isPreUpgradeSnapshotReady(volumeName, snapshotName, log)
}

func (imuc *InstanceManagerUpgradeController) isPreUpgradeSnapshotReady(volumeName, snapshotName string, log *logrus.Entry) (bool, error) {
	snapshot, err := imuc.ds.GetSnapshot(snapshotName)
	if err != nil {
		return false, err
	}
	if snapshot.Status.ReadyToUse && snapshot.Status.Checksum != "" {
		return true, nil
	}
	log.Debugf("Waiting for pre-upgrade snapshot %v of volume %v (checksum computation in progress)", snapshotName, volumeName)
	return false, nil
}

func getPreUpgradeSnapshotName(imuName, volumeName string) string {
	return fmt.Sprintf("upgrade-snapshot-%s", util.GetStringChecksumSHA224(imuName + "/" + volumeName)[:16])
}
