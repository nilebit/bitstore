package manageserver

import (
	"github.com/nilebit/bitstore/diskopt/volume"
	"github.com/nilebit/bitstore/util"
	"sync"
)

type baseNodeInfo struct {
	volumeCount       int
	activeVolumeCount int
	maxVolumeCount    int
	maxVolumeId       util.VIDType
}

type DataNode struct {
	id            	  string
	baseNodeInfo
	sync.RWMutex
	volumeInfos 	  map[util.VIDType]*volume.VolumeInfo
	lastHeartbeat	  int64
}

func NewDataNode(id string) *DataNode {
	return &DataNode{id:id, volumeInfos: map[util.VIDType]*volume.VolumeInfo{}}
}

