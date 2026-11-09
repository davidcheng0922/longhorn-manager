package controller

import (
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const testSharedBlockNode3 = "test-node-name-3"

func newSharedBlockVolume() *longhorn.Volume {
	v := newVolume(TestVolumeName, 3)
	v.Spec.DataEngine = longhorn.DataEngineTypeV2
	v.Spec.AccessMode = longhorn.AccessModeReadWriteMany
	v.Spec.SharedBlock = true
	v.Spec.NodeID = TestNode1
	v.Status.CurrentNodeID = TestNode1
	v.Status.State = longhorn.VolumeStateAttached
	return v
}

func newSharedBlockAttachmentTicket(id string, attacherType longhorn.AttacherType, nodeID string) *longhorn.AttachmentTicket {
	return &longhorn.AttachmentTicket{
		ID:     id,
		Type:   attacherType,
		NodeID: nodeID,
		Parameters: map[string]string{
			longhorn.AttachmentParameterDisableFrontend: longhorn.FalseValue,
		},
	}
}

func setEngineFrontendReady(ef *longhorn.EngineFrontend) {
	ef.Spec.DesireState = longhorn.InstanceStateRunning
	ef.Status.CurrentState = longhorn.InstanceStateRunning
	ef.Status.Endpoint = "/dev/longhorn/" + ef.Spec.VolumeName
}

func (s *TestSuite) TestReconcileSharedBlockEngineFrontends(c *C) {
	skipListerCheck := datastore.SkipListerCheck
	datastore.SkipListerCheck = true
	defer func() {
		datastore.SkipListerCheck = skipListerCheck
	}()

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	vaIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().VolumeAttachments().Informer().GetIndexer()

	vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
	c.Assert(err, IsNil)

	v := newSharedBlockVolume()

	e := newEngineForVolume(v)
	e.Spec.NodeID = TestNode1
	e.Status.CurrentState = longhorn.InstanceStateRunning
	e.Status.StorageIP = "10.0.0.1"
	e.Status.Port = 20001
	es := map[string]*longhorn.Engine{e.Name: e}

	primary := newEngineFrontendForVolume(v, e.Name, TestNode1, "")
	setEngineFrontendReady(primary)
	efs := map[string]*longhorn.EngineFrontend{primary.Name: primary}

	va := newVolumeAttachment(v.Name)
	disableFrontendTicket := newSharedBlockAttachmentTicket("ui-node-3", longhorn.AttacherTypeLonghornAPI, testSharedBlockNode3)
	disableFrontendTicket.Parameters[longhorn.AttachmentParameterDisableFrontend] = longhorn.TrueValue
	va.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"csi-node-1":      newSharedBlockAttachmentTicket("csi-node-1", longhorn.AttacherTypeCSIAttacher, TestNode1),
		"csi-node-2":      newSharedBlockAttachmentTicket("csi-node-2", longhorn.AttacherTypeCSIAttacher, TestNode2),
		"snapshot-node-3": newSharedBlockAttachmentTicket("snapshot-node-3", longhorn.AttacherTypeSnapshotController, testSharedBlockNode3),
		"ui-node-3":       disableFrontendTicket,
	}
	c.Assert(vaIndexer.Add(va), IsNil)

	// A new secondary engine frontend waits for the expansion to complete.
	v.Status.ExpansionRequired = true
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 1)
	v.Status.ExpansionRequired = false

	// Only the workload ticket on a node other than the engine node gets a
	// secondary engine frontend, connected to the same engine target.
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 2)
	secondaryName := types.GenerateSharedBlockEngineFrontendNameForVolume(v.Name, TestNode2)
	secondary := efs[secondaryName]
	c.Assert(secondary, NotNil)
	c.Assert(secondary.Spec.Active, Equals, false)
	c.Assert(secondary.Spec.NodeID, Equals, TestNode2)
	c.Assert(secondary.Spec.EngineName, Equals, e.Name)
	c.Assert(secondary.Spec.TargetIP, Equals, e.Status.StorageIP)
	c.Assert(secondary.Spec.TargetPort, Equals, e.Status.Port)
	c.Assert(secondary.Spec.DesireState, Equals, longhorn.InstanceStateRunning)
	c.Assert(v.Status.FrontendNodeIDs, DeepEquals, []string{TestNode1})

	setEngineFrontendReady(secondary)
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 2)
	c.Assert(v.Status.FrontendNodeIDs, DeepEquals, []string{TestNode1, TestNode2})

	// The secondary engine frontend is removed with its ticket, while the
	// current engine frontend stays on the engine node.
	delete(va.Spec.AttachmentTickets, "csi-node-2")
	c.Assert(vaIndexer.Update(va), IsNil)
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 1)
	c.Assert(efs[primary.Name], NotNil)
	c.Assert(v.Status.FrontendNodeIDs, DeepEquals, []string{TestNode1})

	// Secondary engine frontends are removed while the engine target is not
	// exposed, e.g. when the volume is detaching.
	va.Spec.AttachmentTickets["csi-node-2"] = newSharedBlockAttachmentTicket("csi-node-2", longhorn.AttacherTypeCSIAttacher, TestNode2)
	c.Assert(vaIndexer.Update(va), IsNil)
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 2)
	v.Status.State = longhorn.VolumeStateDetaching
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 1)
	c.Assert(efs[primary.Name], NotNil)

	// A failed secondary engine frontend is removed, and a new one with the
	// same name is only created after the old one is gone.
	v.Status.State = longhorn.VolumeStateAttached
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 2)
	efs[secondaryName].Status.CurrentState = longhorn.InstanceStateError
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 1)
	removing := newEngineFrontendForVolume(v, e.Name, TestNode2, "")
	removing.Name = secondaryName
	removing.Spec.Active = false
	removing.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	efs[removing.Name] = removing
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, es, efs), IsNil)
	c.Assert(efs, HasLen, 2)
	c.Assert(efs[secondaryName], Equals, removing)
}

func (s *TestSuite) TestReconcileSharedBlockEngineFrontendsIgnoresOtherVolumes(c *C) {
	v := newSharedBlockVolume()
	v.Spec.SharedBlock = false

	e := newEngineForVolume(v)
	primary := newEngineFrontendForVolume(v, e.Name, TestNode1, "")
	efs := map[string]*longhorn.EngineFrontend{primary.Name: primary}

	vc := &VolumeController{}
	c.Assert(vc.reconcileSharedBlockEngineFrontends(v, map[string]*longhorn.Engine{e.Name: e}, efs), IsNil)
	c.Assert(efs, HasLen, 1)
	c.Assert(v.Status.FrontendNodeIDs, IsNil)
}

func (s *TestSuite) TestIsEngineFrontendServingVolumeSize(c *C) {
	v := newSharedBlockVolume()
	e := newEngineForVolume(v)
	primary := newEngineFrontendForVolume(v, e.Name, TestNode1, "")
	setEngineFrontendReady(primary)
	primary.Status.CurrentSize = v.Spec.Size
	secondary := newEngineFrontendForVolume(v, e.Name, TestNode2, "")
	secondary.Name = types.GenerateSharedBlockEngineFrontendNameForVolume(v.Name, TestNode2)
	secondary.Spec.Active = false
	setEngineFrontendReady(secondary)
	efs := map[string]*longhorn.EngineFrontend{primary.Name: primary, secondary.Name: secondary}

	// A ready secondary engine frontend still at the old size holds the
	// expansion, while one that is not ready does not.
	served, err := isEngineFrontendServingVolumeSize(v, efs)
	c.Assert(err, IsNil)
	c.Assert(served, Equals, false)

	secondary.Status.CurrentState = longhorn.InstanceStateStopped
	served, err = isEngineFrontendServingVolumeSize(v, efs)
	c.Assert(err, IsNil)
	c.Assert(served, Equals, true)

	setEngineFrontendReady(secondary)
	secondary.Status.CurrentSize = v.Spec.Size
	served, err = isEngineFrontendServingVolumeSize(v, efs)
	c.Assert(err, IsNil)
	c.Assert(served, Equals, true)
}

func (s *TestSuite) TestShouldDoDetachSharedBlockVolume(c *C) {
	vac := &VolumeAttachmentController{baseController: newBaseController("test-va-controller", logrus.StandardLogger())}

	v := newSharedBlockVolume()
	va := newVolumeAttachment(v.Name)

	// Tickets on other nodes do not detach the engine from the engine node,
	// even if the engine node itself is no longer requested.
	va.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"csi-node-2": newSharedBlockAttachmentTicket("csi-node-2", longhorn.AttacherTypeCSIAttacher, TestNode2),
	}
	c.Assert(vac.shouldDoDetach(va, v), Equals, false)

	va.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{}
	c.Assert(vac.shouldDoDetach(va, v), Equals, true)

	// A volume attached with the frontend disabled, e.g. for a restore,
	// detaches once only workload tickets remain, so it can be reattached with
	// the frontend enabled.
	v.Spec.DisableFrontend = true
	restoreTicket := newSharedBlockAttachmentTicket("restore", longhorn.AttacherTypeVolumeRestoreController, TestNode1)
	restoreTicket.Parameters[longhorn.AttachmentParameterDisableFrontend] = longhorn.TrueValue
	va.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"restore":    restoreTicket,
		"csi-node-2": newSharedBlockAttachmentTicket("csi-node-2", longhorn.AttacherTypeCSIAttacher, TestNode2),
	}
	c.Assert(vac.shouldDoDetach(va, v), Equals, false)
	delete(va.Spec.AttachmentTickets, "restore")
	c.Assert(vac.shouldDoDetach(va, v), Equals, true)
	v.Spec.DisableFrontend = false

	// The same tickets detach a regular volume that is attached elsewhere.
	v.Spec.SharedBlock = false
	va.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"csi-node-2": newSharedBlockAttachmentTicket("csi-node-2", longhorn.AttacherTypeCSIAttacher, TestNode2),
	}
	c.Assert(vac.shouldDoDetach(va, v), Equals, true)
}

func (s *TestSuite) TestIsWorkloadTicketOfSharedBlockVolume(c *C) {
	v := newSharedBlockVolume()
	for _, tc := range []struct {
		attacherType longhorn.AttacherType
		expected     bool
	}{
		{longhorn.AttacherTypeCSIAttacher, true},
		{longhorn.AttacherTypeLonghornAPI, true},
		{longhorn.AttacherTypeSnapshotController, false},
		{longhorn.AttacherTypeBackupController, false},
	} {
		ticket := newSharedBlockAttachmentTicket("ticket", tc.attacherType, TestNode2)
		c.Assert(isWorkloadTicketOfSharedBlockVolume(ticket, v), Equals, tc.expected)
	}

	v.Spec.SharedBlock = false
	ticket := newSharedBlockAttachmentTicket("ticket", longhorn.AttacherTypeCSIAttacher, TestNode2)
	c.Assert(isWorkloadTicketOfSharedBlockVolume(ticket, v), Equals, false)
}
