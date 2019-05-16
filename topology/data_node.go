package topology

import (
	"fmt"

	"github.com/nilebit/bitstore/storage"
)

type DataNode struct {
	Ip        string
	Port      int
	PublicUrl string
	LastSeen  int64 // unix time in seconds
}

func NewDataNode(id string) *DataNode {
	s := &DataNode{}
	s.id = NodeId(id)
	s.nodeType = "DataNode"
	s.volumes = make(map[storage.VolumeId]storage.VolumeInfo)
	s.NodeImpl.value = s
	return s
}

func (dn *DataNode) String() string {
	dn.RLock()
	defer dn.RUnlock()
	return fmt.Sprintf("Node:%s, volumes:%v, Ip:%s, Port:%d, PublicUrl:%s", dn.NodeImpl.String(), dn.volumes, dn.Ip, dn.Port, dn.PublicUrl)
}
