package volume

import (
	"github.com/nilebit/bitstore/diskopt/replicate"
	"github.com/nilebit/bitstore/diskopt/ttl"
)

type GrowOption struct {
	Collection       string
	ReplicaPlacement *replicate.Placement
	Ttl              *ttl.TTL
	DataCenter       string
	Rack             string
	DataNode         string
}

