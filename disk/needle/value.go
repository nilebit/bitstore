package needle

type Key uint64

type NeedleValue struct {
	Key    Key
	Offset uint32 `comment:"Volume offset"` //since aligned to 8 bytes, range is 4G*8=32G
	Size   uint32 `comment:"Size of the data portion"`
}

type NeedleValueMap interface {
	Set(key Key, offset, size uint32) (oldOffset, oldSize uint32)
	Delete(key Key) uint32
	Get(key Key) (*NeedleValue, bool)
	Visit(visit func(NeedleValue) error) error
}

