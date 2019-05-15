package needle

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/crc"
	"github.com/nilebit/bitstore/diskopt/ttl"
	"github.com/nilebit/bitstore/diskopt/version"
	"github.com/nilebit/bitstore/util"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
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

func parseMultipart(r *http.Request) (
	fileName string, data []byte, mimeType string, isGzipped bool, originalDataSize int, e error) {
	defer func() {
		if e != nil && r.Body != nil {
			io.Copy(ioutil.Discard, r.Body)
			r.Body.Close()
		}
	}()
	form, fe := r.MultipartReader()
	if fe != nil {
		glog.V(0).Infoln("MultipartReader [ERROR]", fe)
		e = fe
		return
	}

	//first multi-part item
	part, fe := form.NextPart()
	if fe != nil {
		glog.V(0).Infoln("Reading Multi part [ERROR]", fe)
		e = fe
		return
	}

	fileName = part.FileName()
	if fileName != "" {
		fileName = path.Base(fileName)
	}

	data, e = ioutil.ReadAll(part)
	if e != nil {
		glog.V(0).Infoln("Reading Content [ERROR]", e)
		return
	}

	//if the filename is empty string, do a search on the other multi-part items
	for fileName == "" {
		part2, fe := form.NextPart()
		if fe != nil {
			break // no more or on error, just safely break
		}

		fName := part2.FileName()

		//found the first <file type> multi-part has filename
		if fName != "" {
			data2, fe2 := ioutil.ReadAll(part2)
			if fe2 != nil {
				glog.V(0).Infoln("Reading Content [ERROR]", fe2)
				e = fe2
				return
			}

			//update
			data = data2
			fileName = path.Base(fName)
			break
		}
	}

	originalDataSize = len(data)

	return
}

func ParseUpload(r *http.Request) (
	fileName string, data []byte,
	mimeType string, pairMap map[string]string,
	isGzipped bool, originalDataSize int,
	modifiedTime uint64,
	TL *ttl.TTL,
	e error) {

	pairMap = make(map[string]string)
	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, PairNamePrefix) {
			pairMap[k] = v[0]
		}
	}
	if r.Method == "POST" {
		fileName, data, mimeType, isGzipped, originalDataSize, e = parseMultipart(r)
	} else {
		isGzipped = false
		mimeType = r.Header.Get("Content-Type")
		fileName = ""
		data, e = ioutil.ReadAll(r.Body)
		originalDataSize = len(data)
	}
	if e != nil {
		return
	}

	modifiedTime, _ = strconv.ParseUint(r.FormValue("ts"), 10, 64)
	TL, _ = ttl.ReadTTL(r.FormValue("ttl"))

	return
}

func ParseKeyHash(key_hash_string string) (uint64, uint32, error) {
	if len(key_hash_string) <= 8 {
		return 0, 0, fmt.Errorf("KeyHash is too short.")
	}
	if len(key_hash_string) > 24 {
		return 0, 0, fmt.Errorf("KeyHash is too long.")
	}
	split := len(key_hash_string) - 8
	key, err := strconv.ParseUint(key_hash_string[:split], 16, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("Parse key error: %v", err)
	}
	hash, err := strconv.ParseUint(key_hash_string[split:], 16, 32)
	if err != nil {
		return 0, 0, fmt.Errorf("Parse hash error: %v", err)
	}
	return key, uint32(hash), nil
}

func (n *Needle) ParsePath(fid string) (err error) {
	length := len(fid)
	if length <= 8 {
		return fmt.Errorf("Invalid fid: %s", fid)
	}
	delta := ""
	deltaIndex := strings.LastIndex(fid, "_")
	if deltaIndex > 0 {
		fid, delta = fid[0:deltaIndex], fid[deltaIndex+1:]
	}
	n.Id, n.Cookie, err = ParseKeyHash(fid)
	if err != nil {
		return err
	}
	if delta != "" {
		if d, e := strconv.ParseUint(delta, 10, 64); e == nil {
			n.Id += d
		} else {
			return e
		}
	}
	return err
}

func NewNeedle(r *http.Request) (n *Needle, originalSize int, e error) {
	var pairMap map[string]string
	fName, mimeType, isGzipped := "", "", false
	n = new(Needle)
	fName, n.Data, mimeType, pairMap, isGzipped, originalSize, n.LastModified, n.Ttl, e = ParseUpload(r)
	if e != nil {
		return
	}
	if len(fName) < 256 {
		n.Name = []byte(fName)
		n.SetHasName()
	}
	if len(mimeType) < 256 {
		n.Mime = []byte(mimeType)
		n.SetHasMime()
	}
	if len(pairMap) != 0 {
		trimmedPairMap := make(map[string]string)
		for k, v := range pairMap {
			trimmedPairMap[k[len(PairNamePrefix):]] = v
		}

		pairs, _ := json.Marshal(trimmedPairMap)
		if len(pairs) < 65536 {
			n.Pairs = pairs
			n.PairsSize = uint16(len(pairs))
			n.SetHasPairs()
		}
	}
	if isGzipped {
		n.SetGzipped()
	}
	if n.LastModified == 0 {
		n.LastModified = uint64(time.Now().Unix())
	}
	n.SetHasLastModifiedDate()
	if n.Ttl != ttl.EMPTY_TTL {
		n.SetHasTtl()
	}

	// TODO
	n.Checksum = crc.New(n.Data)

	commaSep := strings.LastIndex(r.URL.Path, ",")
	dotSep := strings.LastIndex(r.URL.Path, ".")
	fid := r.URL.Path[commaSep+1:]
	if dotSep > 0 {
		fid = r.URL.Path[commaSep+1 : dotSep]
	}

	e = n.ParsePath(fid)

	return
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

func (n *Needle) Etag() string {
	bits := make([]byte, 4)
	util.Uint32toBytes(bits, uint32(n.Checksum))
	return fmt.Sprintf("\"%x\"", bits)
}

func (n *Needle) ReleaseMemory() {
	if n.rawBlock != nil {
		n.rawBlock.decreaseReference()
	}
}

func (n *Needle) Append(w io.Writer, vers version.Version) (size uint32, actualSize int64, err error) {
	if s, ok := w.(io.Seeker); ok {
		if end, e := s.Seek(0, 1); e == nil {
			defer func(s io.Seeker, off int64) {
				if err != nil {
					if _, e = s.Seek(off, 0); e != nil {
						glog.V(0).Infof("Failed to seek %s back to %d with error: %v", w, off, e)
					}
				}
			}(s, end)
		} else {
			err = fmt.Errorf("Cannot Read Current Volume Position: %v", e)
			return
		}
	}
	switch vers {
	case version.Version1:
		header := make([]byte, HeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.Size = uint32(len(n.Data))
		size = n.Size
		util.Uint32toBytes(header[12:16], n.Size)
		if _, err = w.Write(header); err != nil {
			return
		}
		if _, err = w.Write(n.Data); err != nil {
			return
		}
		actualSize = HeaderSize + int64(n.Size)
		padding := PaddingSize - ((HeaderSize + n.Size + ChecksumSize) % PaddingSize)
		util.Uint32toBytes(header[0:ChecksumSize], n.Checksum.Value())
		_, err = w.Write(header[0:ChecksumSize+padding])
		return
	case version.Version2:
		header := make([]byte, HeaderSize)
		util.Uint32toBytes(header[0:4], n.Cookie)
		util.Uint64toBytes(header[4:12], n.Id)
		n.DataSize, n.NameSize, n.MimeSize = uint32(len(n.Data)), uint8(len(n.Name)), uint8(len(n.Mime))
		if n.DataSize > 0 {
			n.Size = 4 + n.DataSize + 1
			if n.HasName() {
				n.Size = n.Size + 1 + uint32(n.NameSize)
			}
			if n.HasMime() {
				n.Size = n.Size + 1 + uint32(n.MimeSize)
			}
			if n.HasLastModifiedDate() {
				n.Size = n.Size + LastModifiedBytesLength
			}
			if n.HasTtl() {
				n.Size = n.Size + TtlBytesLength
			}
			if n.HasPairs() {
				n.Size += 2 + uint32(n.PairsSize)
			}
		} else {
			n.Size = 0
		}
		size = n.DataSize
		util.Uint32toBytes(header[12:16], n.Size)
		if _, err = w.Write(header); err != nil {
			return
		}
		if n.DataSize > 0 {
			util.Uint32toBytes(header[0:4], n.DataSize)
			if _, err = w.Write(header[0:4]); err != nil {
				return
			}
			if _, err = w.Write(n.Data); err != nil {
				return
			}
			util.Uint8toBytes(header[0:1], n.Flags)
			if _, err = w.Write(header[0:1]); err != nil {
				return
			}
			if n.HasName() {
				util.Uint8toBytes(header[0:1], n.NameSize)
				if _, err = w.Write(header[0:1]); err != nil {
					return
				}
				if _, err = w.Write(n.Name); err != nil {
					return
				}
			}
			if n.HasMime() {
				util.Uint8toBytes(header[0:1], n.MimeSize)
				if _, err = w.Write(header[0:1]); err != nil {
					return
				}
				if _, err = w.Write(n.Mime); err != nil {
					return
				}
			}
			if n.HasLastModifiedDate() {
				util.Uint64toBytes(header[0:8], n.LastModified)
				if _, err = w.Write(header[8-LastModifiedBytesLength : 8]); err != nil {
					return
				}
			}
			if n.HasTtl() && n.Ttl != nil {
				n.Ttl.ToBytes(header[0:TtlBytesLength])
				if _, err = w.Write(header[0:TtlBytesLength]); err != nil {
					return
				}
			}
			if n.HasPairs() {
				util.Uint16toBytes(header[0:2], n.PairsSize)
				if _, err = w.Write(header[0:2]); err != nil {
					return
				}
				if _, err = w.Write(n.Pairs); err != nil {
					return
				}
			}
		}
		padding := PaddingSize - ((HeaderSize + n.Size + ChecksumSize) % PaddingSize)
		util.Uint32toBytes(header[0:ChecksumSize], n.Checksum.Value())
		_, err = w.Write(header[0:ChecksumSize+padding])

		return n.DataSize, getActualSize(n.Size), err
	}
	return 0, 0, fmt.Errorf("Unsupported Version! (%d)", vers)
}