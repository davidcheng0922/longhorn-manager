package engineapi

import (
	"fmt"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
)

func (p *Proxy) VolumeGet(e *longhorn.Engine) (volume *Volume, err error) {
	engineFrontendName := ""
	if types.IsDataEngineV2(e.Spec.DataEngine) && p.ds != nil {
		if ef, err := p.ds.GetVolumeCurrentEngineFrontend(e.Spec.VolumeName); err == nil && ef != nil {
			engineFrontendName = ef.Name
		}
	}

	recv, err := p.grpcClient.VolumeGet(string(e.Spec.DataEngine), e.Name, engineFrontendName, e.Spec.VolumeName, p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	return (*Volume)(recv), nil
}

func (p *Proxy) VolumeExpand(obj DataEngineObject) (err error) {
	return p.grpcClient.VolumeExpand(obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(), p.DirectToURL(obj), obj.GetVolumeSize())
}

func (p *Proxy) VolumeFrontendStart(e *longhorn.Engine) (err error) {
	frontendName, err := GetEngineInstanceFrontend(e.Spec.DataEngine, e.Spec.Frontend)
	if err != nil {
		return err
	}

	if frontendName == "" {
		return fmt.Errorf("cannot start empty frontend")
	}

	return p.grpcClient.VolumeFrontendStart(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), frontendName)
}

func (p *Proxy) VolumeFrontendShutdown(e *longhorn.Engine) (err error) {
	return p.grpcClient.VolumeFrontendShutdown(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e))
}

func (p *Proxy) VolumeUnmapMarkSnapChainRemovedSet(e *longhorn.Engine) error {
	return p.grpcClient.VolumeUnmapMarkSnapChainRemovedSet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), e.Spec.UnmapMarkSnapChainRemovedEnabled)
}

func (p *Proxy) VolumeSnapshotMaxCountSet(e *longhorn.Engine) error {
	return p.grpcClient.VolumeSnapshotMaxCountSet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), e.Spec.SnapshotMaxCount)
}

func (p *Proxy) VolumeSnapshotMaxSizeSet(e *longhorn.Engine) error {
	return p.grpcClient.VolumeSnapshotMaxSizeSet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), e.Spec.SnapshotMaxSize)
}

func (p *Proxy) RemountReadOnlyVolume(e *longhorn.Engine) error {
	return p.grpcClient.RemountReadOnlyVolume(e.Spec.VolumeName)
}
