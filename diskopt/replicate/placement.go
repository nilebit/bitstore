package replicate

import (
	"errors"
	"fmt"
)

type Placement struct {
	SameRackCount       int
	DiffRackCount       int
	DiffDataCenterCount int
}

func NewPlacementFromString(t string) (*Placement, error) {
	rp := &Placement{}
	for i, c := range t {
		count := int(c - '0')
		if 0 <= count && count <= 2 {
			switch i {
			case 0:
				rp.DiffDataCenterCount = count
			case 1:
				rp.DiffRackCount = count
			case 2:
				rp.SameRackCount = count
			}
		} else {
			return rp, errors.New("Unknown Replication Type:" + t)
		}
	}
	return rp, nil
}

func NewPlacementFromByte(b byte) (*Placement, error) {
	return NewPlacementFromString(fmt.Sprintf("%03d", b))
}

func (rp *Placement) Byte() byte {
	ret := rp.DiffDataCenterCount*100 + rp.DiffRackCount*10 + rp.SameRackCount
	return byte(ret)
}

func (rp *Placement) GetCopyCount() int {
	return rp.DiffDataCenterCount + rp.DiffRackCount + rp.SameRackCount + 1
}

func (rp *Placement) String() string {
	b := make([]byte, 3)
	b[0] = byte(rp.DiffDataCenterCount + '0')
	b[1] = byte(rp.DiffRackCount + '0')
	b[2] = byte(rp.SameRackCount + '0')
	return string(b)
}
