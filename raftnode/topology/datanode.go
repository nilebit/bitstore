package topology

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/util"
	"strconv"
)

type DataNode struct {
	NodeImpl
	volumes   map[util.VIDType]VolumeInfo
	Ip        string
	Port      int
	PublicUrl string
	LastSeen  int64 // unix time in seconds
}

func NewDataNode(id string) *DataNode {
	s := &DataNode{}
	s.id = NodeId(id)
	s.nodeType = "DataNode"
	s.volumes = make(map[util.VIDType]VolumeInfo)
	s.NodeImpl.value = s
	return s
}

func (dn *DataNode) String() string {
	dn.RLock()
	defer dn.RUnlock()
	return fmt.Sprintf("Node:%s, volumes:%v, Ip:%s, Port:%d, PublicUrl:%s", dn.NodeImpl.String(), dn.volumes, dn.Ip, dn.Port, dn.PublicUrl)
}

func (dn *DataNode) AddOrUpdateVolume(v VolumeInfo) {
	dn.Lock()
	defer dn.Unlock()
	if _, ok := dn.volumes[v.Id]; !ok {
		dn.volumes[v.Id] = v
		dn.UpAdjustVolumeCountDelta(1)
		if !v.ReadOnly {
			dn.UpAdjustActiveVolumeCountDelta(1)
		}
		dn.UpAdjustMaxVolumeId(v.Id)
	} else {
		dn.volumes[v.Id] = v
	}
}

func (dn *DataNode) UpdateVolumes(actualVolumes []VolumeInfo) (deletedVolumes []VolumeInfo) {
	actualVolumeMap := make(map[util.VIDType]VolumeInfo)
	for _, v := range actualVolumes {
		actualVolumeMap[v.Id] = v
	}
	dn.Lock()
	for vid, v := range dn.volumes {
		if _, ok := actualVolumeMap[vid]; !ok {
			glog.V(0).Infoln("Deleting volume id:", vid)
			delete(dn.volumes, vid)
			deletedVolumes = append(deletedVolumes, v)
			dn.UpAdjustVolumeCountDelta(-1)
			dn.UpAdjustActiveVolumeCountDelta(-1)
		}
	}
	dn.Unlock()
	for _, v := range actualVolumes {
		dn.AddOrUpdateVolume(v)
	}
	return
}

func (dn *DataNode) GetVolumes() (ret []VolumeInfo) {
	dn.RLock()
	for _, v := range dn.volumes {
		ret = append(ret, v)
	}
	dn.RUnlock()
	return ret
}

func (dn *DataNode) GetVolumesById(id util.VIDType) (VolumeInfo, error) {
	dn.RLock()
	defer dn.RUnlock()
	v_info, ok := dn.volumes[id]
	if ok {
		return v_info, nil
	} else {
		return VolumeInfo{}, fmt.Errorf("volumeInfo not found")
	}
}

func (dn *DataNode) GetDataCenter() *DataCenter {
	return dn.Parent().Parent().(*NodeImpl).value.(*DataCenter)
}

func (dn *DataNode) GetRack() *Rack {
	return dn.Parent().(*NodeImpl).value.(*Rack)
}

func (dn *DataNode) GetTopology() *TopoStore {
	p := dn.Parent()
	for p.Parent() != nil {
		p = p.Parent()
	}
	t := p.(*TopoStore)
	return t
}

func (dn *DataNode) MatchLocation(ip string, port int) bool {
	return dn.Ip == ip && dn.Port == port
}

func (dn *DataNode) Url() string {
	return dn.Ip + ":" + strconv.Itoa(dn.Port)
}

func (dn *DataNode) ToMap() interface{} {
	ret := make(map[string]interface{})
	ret["Url"] = dn.Url()
	ret["Volumes"] = dn.GetVolumeCount()
	ret["Max"] = dn.GetMaxVolumeCount()
	ret["Free"] = dn.FreeSpace()
	ret["PublicUrl"] = dn.PublicUrl
	return ret
}
