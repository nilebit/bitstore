package diskopt

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/needle"
	"github.com/nilebit/bitstore/diskopt/volume"
	"github.com/nilebit/bitstore/util"
)

type Disk struct {
	Locations       	[]*Location
	VolumeSizeLimit 	uint64
	Ip                 	string
	Port               	int
}

// NewDisk 新建磁盘
func NewDisk(dirNames []string, maxVolumeCounts []int, ip string, port int) (v *Disk) {
	v = &Disk{Ip:ip, Port:port}
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
