package volume

import (
	"fmt"
	"sort"

	"github.com/nilebit/bitstore/diskopt/ttl"
	"github.com/nilebit/bitstore/pb"

	"github.com/nilebit/bitstore/diskopt/replicate"
	"github.com/nilebit/bitstore/diskopt/version"
	"github.com/nilebit/bitstore/util"
)

type VolumeInfo struct {
	Id               util.VIDType `json:"ID"`
	Size             uint64 `json:"Size"`
	ReplicaPlacement *replicate.Placement
	Ttl              *ttl.TTL `json:"TTL"`
	Collection       string
	Version          version.Version `json:"Version"`
	FileCount        int `json:"FileCount"`
	DeleteCount      int `json:"DeleteCount"`
	DeletedByteCount uint64 `json:"DeletedByteCount"`
	ReadOnly         bool `json:"ReadOnly"`
}

func NewVolumeInfo(m *pb.VolumeInformationMessage) (vi VolumeInfo, err error) {
	vi = VolumeInfo{
		Id:               util.VIDType(m.Id),
		Size:             m.Size,
		Collection:       m.Collection,
		FileCount:        int(m.FileCount),
		DeleteCount:      int(m.DeleteCount),
		DeletedByteCount: m.DeletedByteCount,
		ReadOnly:         m.ReadOnly,
		Version:          version.Version(m.Version),
	}
	rp, e := replicate.NewPlacementFromByte(byte(m.ReplicaPlacement))
	if e != nil {
		return vi, e
	}
	vi.ReplicaPlacement = rp
	vi.Ttl = ttl.LoadFromUint32(m.Ttl)
	return vi, nil
}

func (vi VolumeInfo) String() string {
	return fmt.Sprintf("Id:%d, Size:%d, ReplicaPlacement:%s, Collection:%s, Version:%v, FileCount:%d, DeleteCount:%d, DeletedByteCount:%d, ReadOnly:%v",
		vi.Id, vi.Size, vi.ReplicaPlacement, vi.Collection, vi.Version, vi.FileCount, vi.DeleteCount, vi.DeletedByteCount, vi.ReadOnly)
}

/*VolumesInfo sorting*/

type volumeInfos []*VolumeInfo

func (vis volumeInfos) Len() int {
	return len(vis)
}

func (vis volumeInfos) Less(i, j int) bool {
	return vis[i].Id < vis[j].Id
}

func (vis volumeInfos) Swap(i, j int) {
	vis[i], vis[j] = vis[j], vis[i]
}

func SortVolumeInfos(vis volumeInfos) {
	sort.Sort(vis)
}
