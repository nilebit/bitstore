package diskopt

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nilebit/bitstore/diskopt/replicate"
	"github.com/nilebit/bitstore/diskopt/ttl"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/needle"
	"github.com/nilebit/bitstore/diskopt/volume"
	"github.com/nilebit/bitstore/util"
)

type Disk struct {
	Locations       []*Location
	VolumeSizeLimit uint64
	Ip              string
	Port            int
}

// NewDisk 新建磁盘
func NewDisk(dirNames []string, maxVolumeCounts []int, ip string, port int) (v *Disk) {
	v = &Disk{Ip: ip, Port: port}
	v.Locations = make([]*Location, 0)
	for i := 0; i < len(dirNames); i++ {
		location := NewLocation(dirNames[i], maxVolumeCounts[i])
		location.loadExistingVolumes()
		v.Locations = append(v.Locations, location)
	}
	return
}

func (d *Disk) FindVolume(vid util.VIDType) *volume.Volume {
	for _, location := range d.Locations {
		if v, found := location.FindVolume(vid); found {
			return v
		}
	}
	return nil
}

func (d *Disk) Write(i util.VIDType, n *needle.Needle) (size uint32, isUnchanged bool, err error) {
	if v := d.FindVolume(i); v != nil {
		if v.ReadOnly {
			err = fmt.Errorf("Volume %d is read only", i)
			return
		}
		// TODO
		if needle.MaxPossibleVolumeSize >= v.ContentSize()+uint64(size) {
			size, isUnchanged, err = v.WriteNeedle(n)
		} else {
			err = fmt.Errorf("Volume Size Limit %d Exceeded! Current size is %d", d.VolumeSizeLimit, v.ContentSize())
		}
		return
	}
	glog.V(0).Infoln("volume", i, "not found!")
	err = fmt.Errorf("Volume %d not found!", i)
	return
}

func (d *Disk) HasVolume(i util.VIDType) bool {
	v := d.FindVolume(i)
	return v != nil
}

func (d *Disk) ReadVolumeNeedle(i util.VIDType, n *needle.Needle) (int, error) {
	if v := d.FindVolume(i); v != nil {
		return v.ReadNeedle(n)
	}
	return 0, fmt.Errorf("Volume %d not found!", i)
}

func (s *Disk) Delete(i util.VIDType, n *needle.Needle) (uint32, error) {
	if v := s.FindVolume(i); v != nil && !v.ReadOnly {
		return v.DeleteNeedle(n)
	}
	return 0, nil
}

func (s *Disk) Status() []*volume.VolumeInfo {
	var stats []*volume.VolumeInfo
	for _, location := range s.Locations {
		location.RLock()
		for k, v := range location.Volumes {
			s := &volume.VolumeInfo{
				Id:               util.VIDType(k),
				Size:             v.ContentSize(),
				Collection:       v.Collection,
				ReplicaPlacement: v.ReplicaPlacement,
				Version:          v.Version(),
				FileCount:        v.NM.FileCount(),
				DeleteCount:      v.NM.DeletedCount(),
				DeletedByteCount: v.NM.DeletedSize(),
				ReadOnly:         v.ReadOnly,
				Ttl:              v.Ttl}
			stats = append(stats, s)
		}
		location.RUnlock()
	}
	volume.SortVolumeInfos(stats)
	return stats
}

func (s *Disk) findFreeLocation() (ret *Location) {
	max := 0
	for _, location := range s.Locations {
		currentFreeCount := location.MaxVolumeCount - location.VolumesLen()
		if currentFreeCount > max {
			max = currentFreeCount
			ret = location
		}
	}
	return ret
}

func (s *Disk) addVolume(vid util.VIDType, collection string, replicaPlacement *replicate.Placement, ttl *ttl.TTL, preallocate int64) error {
	if s.FindVolume(vid) != nil {
		return fmt.Errorf("Volume Id %d already exists!", vid)
	}

	if location := s.findFreeLocation(); location != nil {
		glog.V(0).Infof("In dir %s adds volume:%v collection:%s replicaPlacement:%v ttl:%v",
			location.Directory, vid, collection, replicaPlacement, ttl)
		if volume, err := volume.NewVolume(location.Directory, collection, vid, replicaPlacement, ttl, preallocate); err == nil {
			location.SetVolume(vid, volume)
			return nil
		} else {
			return err
		}
	}
	return fmt.Errorf("No more free space left")
}

func (s *Disk) AddVolume(volumeListString string, collection string, replicaPlacement string, ttlString string, preallocate int64) (err error) {
	rt, err := replicate.NewPlacementFromString(replicaPlacement)
	if err != nil {
		return
	}

	ttl, err := ttl.ReadTTL(ttlString)
	if err != nil {
		return
	}

	//now, volumeListString only contains one volume ID
	for _, range_string := range strings.Split(volumeListString, ",") {
		// judge whether volumeListString is Scope of vid
		if strings.Index(range_string, "-") < 0 {
			id_string := range_string
			id, err := volume.NewVolumeId(id_string)
			if err != nil {
				return fmt.Errorf("Volume Id %s is not a valid unsigned integer!", id_string)
			}
			err = s.addVolume(id, collection, rt, ttl, preallocate)
			if err != nil {
				return err
			}
		} else {
			pair := strings.Split(range_string, "-")
			start, start_err := strconv.ParseUint(pair[0], 10, 64)
			if start_err != nil {
				return fmt.Errorf("Volume Start Id %s is not a valid unsigned integer!", pair[0])
			}
			end, end_err := strconv.ParseUint(pair[1], 10, 64)
			if end_err != nil {
				return fmt.Errorf("Volume End Id %s is not a valid unsigned integer!", pair[1])
			}
			for id := start; id <= end; id++ {
				if err = s.addVolume(util.VIDType(id), collection, rt, ttl, preallocate); err != nil {
					return err
				}
			}
		}
	}
	return
}
