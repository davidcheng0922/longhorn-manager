package engineapi

import (
	"testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetObjInfoForVolumeExpand(t *testing.T) {
	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol-e-0",
		},
		Spec: longhorn.EngineSpec{
			InstanceSpec: longhorn.InstanceSpec{
				DataEngine: longhorn.DataEngineTypeV2,
				VolumeName: "vol",
			},
		},
	}

	var obj DataEngineObject = engine
	if obj.GetDataEngine() != string(longhorn.DataEngineTypeV2) || obj.GetEngineName() != "vol-e-0" || obj.GetEngineFrontendName() != "" || obj.GetVolumeName() != "vol" {
		t.Fatalf("Engine DataEngineObject = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
			obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(),
			string(longhorn.DataEngineTypeV2), "vol-e-0", "", "vol")
	}

	engineFrontend := &longhorn.EngineFrontend{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol-ef-0",
		},
		Spec: longhorn.EngineFrontendSpec{
			InstanceSpec: longhorn.InstanceSpec{
				DataEngine: longhorn.DataEngineTypeV2,
				VolumeName: "vol",
			},
			EngineName: "vol-e-0",
		},
	}

	obj = engineFrontend
	if obj.GetDataEngine() != string(longhorn.DataEngineTypeV2) || obj.GetEngineName() != "vol-e-0" || obj.GetEngineFrontendName() != "vol-ef-0" || obj.GetVolumeName() != "vol" {
		t.Fatalf("EngineFrontend DataEngineObject = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
			obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(),
			string(longhorn.DataEngineTypeV2), "vol-e-0", "vol-ef-0", "vol")
	}
}
