package manageserver

import (
	"github.com/nilebit/bitstore/diskopt/volume"
	"github.com/nilebit/bitstore/util"
	"strconv"
	"sync"
)

type baseNodeInfo struct {
	volumeCount       int
	activeVolumeCount int
	MaxVolumeCount    int   `json:"Max"`
	FreeVolumeCount   int   `json:"Free"`
	MaxVolumeId       util.VIDType  `json:"-"`
}

type DataNode struct {
	id            	  string
	baseNodeInfo
	sync.RWMutex
	VolumeInfos 	  map[util.VIDType]*volume.VolumeInfo
	lastHeartbeat	  int64
	Ip        string   `json:"-"`
	Port      int      `json:"-"`
	PublicUrl string   `json:"-"`
}

func NewDataNode(id string) *DataNode {
	return &DataNode{id:id, VolumeInfos: map[util.VIDType]*volume.VolumeInfo{}}
}

func (dn *DataNode) Url() string {
	return dn.Ip + ":" + strconv.Itoa(dn.Port)
}

func (dn *DataNode) AddVolume(v *volume.VolumeInfo) {
	dn.Lock()
	defer dn.Unlock()
	dn.VolumeInfos[v.Id] = v
}