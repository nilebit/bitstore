package diskopt

type Disk struct {
	Locations       []*Location
}

// NewDisk 新建磁盘
func NewDisk(dirNames []string, maxVolumeCounts []int) (v *Disk) {
	v = &Disk{}
	v.Locations = make([]*Location, 0)
	for i := 0; i < len(dirNames); i++ {
		location := NewLocation(dirNames[i], maxVolumeCounts[i])
		location.loadExistingVolumes()
		v.Locations = append(v.Locations, location)
	}
	return
}
