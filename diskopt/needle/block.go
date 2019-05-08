package needle

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/nilebit/bitstore/util"
	"sync/atomic"
)

type Block struct {
	Bytes    []byte
	refCount int32
}

var (
	EnableBytesCache = true
	bytesCache       *lru.Cache
	bytesPool        *util.BytesPool
)

func (block *Block) decreaseReference() {
	if atomic.AddInt32(&block.refCount, -1) == 0 {
		bytesPool.Put(block.Bytes)
	}
}
func (block *Block) increaseReference() {
	atomic.AddInt32(&block.refCount, 1)
}
