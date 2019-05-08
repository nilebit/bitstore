package needle

import (
	"errors"
	"fmt"
	"github.com/nilebit/bitstore/disk/crc"
	"github.com/nilebit/bitstore/disk/ttl"
	"github.com/nilebit/bitstore/disk/version"
	"github.com/nilebit/bitstore/util"
	"math"
	"os"
)

const (
	HeaderSize      = 16 //should never change this
	PaddingSize     = 8
	ChecksumSize    = 4
	MaxPossibleVolumeSize = 4 * 1024 * 1024 * 1024 * 8
	TombstoneFileSize     = math.MaxUint32
	PairNamePrefix        = "Seaweed-"
	IndexSize = 16

	FlagGzip                = 0x01
	FlagHasName             = 0x02
	FlagHasMime             = 0x04
	FlagHasLastModifiedDate = 0x08
	FlagHasTtl              = 0x10
	FlagHasPairs            = 0x20
	FlagIsChunkManifest     = 0x80
	LastModifiedBytesLength = 5
	TtlBytesLength          = 2
)

/*
* A Needle means a uploaded and stored file.
* Needle file size is limited to 4GB for now.
 */
type Needle struct {
	Cookie uint32 `comment:"random number to mitigate brute force lookups"`
	Id     uint64 `comment:"needle id"`
	Size   uint32 `comment:"sum of DataSize,Data,NameSize,Name,MimeSize,Mime"`
	DataSize     uint32 `comment:"Data size"` //version2
	Data         []byte `comment:"The actual file data"`
	Flags        byte   `comment:"boolean flags"` //version2
	NameSize     uint8  //version2
	Name         []byte `comment:"maximum 256 characters"` //version2
	MimeSize     uint8  //version2
	Mime         []byte `comment:"maximum 256 characters"` //version2
	PairsSize    uint16 //version2
	Pairs        []byte `comment:"additional name value pairs, json format, maximum 64kB"`
	LastModified uint64 //only store LastModifiedBytesLength bytes, which is 5 bytes to disk
	Ttl          *ttl.TTL
	Checksum crc.CRC    `comment:"CRC32 to check integrity"`
	Padding  []byte `comment:"Aligned to 8 bytes"`

	rawBlock *Block // underlying supporing []byte, fetched and released into a pool
}

func getActualSize(size uint32) int64 {
	padding := PaddingSize - ((HeaderSize + size + ChecksumSize) % PaddingSize)
	return HeaderSize + int64(size) + ChecksumSize + int64(padding)
}

// get bytes from the LRU cache of []byte first, then from the bytes pool
// when []byte in LRU cache is evicted, it will be put back to the bytes pool
func getBytesForFileBlock(r *os.File, offset int64, readSize int) (dataSlice []byte, block *Block, err error) {
	// check cache, return if found
	cacheKey := fmt.Sprintf("%d:%d:%d", r.Fd(), offset>>3, readSize)
	if EnableBytesCache {
		if obj, found := bytesCache.Get(cacheKey); found {
			block = obj.(*Block)
			block.increaseReference()
			dataSlice = block.Bytes[0:readSize]
			return dataSlice, block, nil
		}
	}

	// get the []byte from pool
	b := bytesPool.Get(readSize)
	// refCount = 2, one by the bytesCache, one by the actual needle object
	block = &Block{Bytes: b, refCount: 2}
	dataSlice = block.Bytes[0:readSize]
	_, err = r.ReadAt(dataSlice, offset)
	if EnableBytesCache {
		bytesCache.Add(cacheKey, block)
	}

	return dataSlice, block, err
}

func (n *Needle) ParseHeader(byte []byte) {
	n.Cookie = util.BytesToUint32(byte[0:4])
	n.Id = util.BytesToUint64(byte[4:12])
	n.Size = util.BytesToUint32(byte[12:HeaderSize])
}

func (n *Needle) ReadData(r *os.File, offset int64, size uint32, vers version.Version) (err error) {
	bytes, block, err := getBytesForFileBlock(r, offset, int(getActualSize(size)))
	if err != nil {
		return err
	}
	n.rawBlock = block
	n.ParseHeader(bytes)
	if n.Size != size {
		return fmt.Errorf("File Entry Not Found. Needle %d Memory %d", n.Size, size)
	}
	switch vers {
	case version.Version1:
		n.Data = bytes[HeaderSize :HeaderSize+size]
	case version.Version2:
		n.readDataVersion2(bytes[HeaderSize : HeaderSize+int(n.Size)])
	}
	if size == 0 {
		return nil
	}
	checksum := util.BytesToUint32(bytes[HeaderSize+size : HeaderSize+size+ChecksumSize])
	newChecksum := crc.New(n.Data)
	if checksum != newChecksum.Value() {
		return errors.New("CRC error! Data On Disk Corrupted")
	}
	n.Checksum = newChecksum
	return nil
}

func (n *Needle) readDataVersion2(bytes []byte) {
	index, lenBytes := 0, len(bytes)
	if index < lenBytes {
		n.DataSize = util.BytesToUint32(bytes[index : index+4])
		index = index + 4
		if int(n.DataSize)+index > lenBytes {
			// this if clause is due to bug #87 and #93, fixed in v0.69
			// remove this clause later
			return
		}
		n.Data = bytes[index : index+int(n.DataSize)]
		index = index + int(n.DataSize)
		n.Flags = bytes[index]
		index = index + 1
	}
	if index < lenBytes && n.HasName() {
		n.NameSize = uint8(bytes[index])
		index = index + 1
		n.Name = bytes[index : index+int(n.NameSize)]
		index = index + int(n.NameSize)
	}
	if index < lenBytes && n.HasMime() {
		n.MimeSize = uint8(bytes[index])
		index = index + 1
		n.Mime = bytes[index : index+int(n.MimeSize)]
		index = index + int(n.MimeSize)
	}
	if index < lenBytes && n.HasLastModifiedDate() {
		n.LastModified = util.BytesToUint64(bytes[index : index+LastModifiedBytesLength])
		index = index + LastModifiedBytesLength
	}
	if index < lenBytes && n.HasTtl() {
		n.Ttl = ttl.LoadFromBytes(bytes[index : index+TtlBytesLength])
		index = index + TtlBytesLength
	}
	if index < lenBytes && n.HasPairs() {
		n.PairsSize = util.BytesToUint16(bytes[index : index+2])
		index += 2
		end := index + int(n.PairsSize)
		n.Pairs = bytes[index:end]
		index = end
	}
}


func (n *Needle) IsGzipped() bool {
	return n.Flags&FlagGzip > 0
}

func (n *Needle) SetGzipped() {
	n.Flags = n.Flags | FlagGzip
}

func (n *Needle) HasName() bool {
	return n.Flags&FlagHasName > 0
}

func (n *Needle) SetHasName() {
	n.Flags = n.Flags | FlagHasName
}

func (n *Needle) HasMime() bool {
	return n.Flags&FlagHasMime > 0
}

func (n *Needle) SetHasMime() {
	n.Flags = n.Flags | FlagHasMime
}

func (n *Needle) HasLastModifiedDate() bool {
	return n.Flags&FlagHasLastModifiedDate > 0
}

func (n *Needle) SetHasLastModifiedDate() {
	n.Flags = n.Flags | FlagHasLastModifiedDate
}

func (n *Needle) HasTtl() bool {
	return n.Flags&FlagHasTtl > 0
}

func (n *Needle) SetHasTtl() {
	n.Flags = n.Flags | FlagHasTtl
}


func (n *Needle) HasPairs() bool {
	return n.Flags&FlagHasPairs != 0
}

func (n *Needle) SetHasPairs() {
	n.Flags = n.Flags | FlagHasPairs
}
