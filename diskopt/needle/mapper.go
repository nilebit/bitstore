package needle

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/util"
	"io"
	"io/ioutil"
	"os"
	"sync"
)
const (
	RowsToRead = 1024
)

type Mapper interface {
	Put(key uint64, offset uint32, size uint32) error
	Get(key uint64) (element *NeedleValue, ok bool)
	Delete(key uint64, offset uint32) error
	Close()
	Destroy() error
	ContentSize() uint64
	DeletedSize() uint64
	FileCount() int
	DeletedCount() int
	MaxFileKey() uint64
	IndexFileSize() uint64
	IndexFileContent() ([]byte, error)
	IndexFileName() string
}

func (nm *NeedleMap) Put(key uint64, offset uint32, size uint32) error {
	_, oldSize := nm.m.Set(Key(key), offset, size)
	nm.logPut(key, oldSize, size)
	return nm.appendToIndexFile(key, offset, size)
}

func (nm *NeedleMap) Get(key uint64) (element *NeedleValue, ok bool) {
	element, ok = nm.m.Get(Key(key))
	return
}

func (nm *NeedleMap) Delete(key uint64, offset uint32) error {
	deletedBytes := nm.m.Delete(Key(key))
	nm.logDelete(deletedBytes)
	return nm.appendToIndexFile(key, offset, TombstoneFileSize)
}

func (nm *NeedleMap) Close() {
	_ = nm.indexFile.Close()
}

func (nm *NeedleMap) Destroy() error {
	nm.Close()
	return os.Remove(nm.indexFile.Name())
}

type mapMetric struct {
	indexFile *os.File

	DeletionCounter     int    `json:"DeletionCounter"`
	FileCounter         int    `json:"FileCounter"`
	DeletionByteCounter uint64 `json:"DeletionByteCounter"`
	FileByteCounter     uint64 `json:"FileByteCounter"`
	MaximumFileKey      uint64 `json:"MaxFileKey"`
}

type baseNeedleMapper struct {
	indexFile           *os.File
	indexFileAccessLock sync.Mutex

	mapMetric
}

type NeedleMap struct {
	m NeedleValueMap
	baseNeedleMapper
}

func NewCompactNeedleMap(file *os.File) *NeedleMap {
	nm := &NeedleMap{
		m: NewCompactMap(),
	}
	nm.indexFile = file
	return nm
}

func IdxFileEntry(bytes []byte) (key uint64, offset uint32, size uint32) {
	key = util.BytesToUint64(bytes[:8])
	offset = util.BytesToUint32(bytes[8:12])
	size = util.BytesToUint32(bytes[12:16])
	return
}

// walks through the index file, calls fn function with each key, offset, size
// stops with the error returned by the fn function
func WalkIndexFile(r *os.File, fn func(key uint64, offset, size uint32) error) error {
	var readerOffset int64
	bytes := make([]byte, 16*RowsToRead)
	count, e := r.ReadAt(bytes, readerOffset)
	glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
	readerOffset += int64(count)
	var (
		key          uint64
		offset, size uint32
		i            int
	)

	for count > 0 && e == nil || e == io.EOF {
		for i = 0; i+16 <= count; i += 16 {
			key, offset, size = IdxFileEntry(bytes[i : i+16])
			if e = fn(key, offset, size); e != nil {
				return e
			}
		}
		if e == io.EOF {
			return nil
		}
		count, e = r.ReadAt(bytes, readerOffset)
		glog.V(3).Infoln("file", r.Name(), "readerOffset", readerOffset, "count", count, "e", e)
		readerOffset += int64(count)
	}
	return e
}

func doLoading(file *os.File, nm *NeedleMap) (*NeedleMap, error) {
	e := WalkIndexFile(file, func(key uint64, offset, size uint32) error {
		if key > nm.MaximumFileKey {
			nm.MaximumFileKey = key
		}
		if offset > 0 && size != TombstoneFileSize {
			nm.FileCounter++
			nm.FileByteCounter = nm.FileByteCounter + uint64(size)
			oldOffset, oldSize := nm.m.Set(Key(key), offset, size)
			glog.V(3).Infoln("reading key", key, "offset", offset*PaddingSize, "size", size, "oldSize", oldSize)
			if oldOffset > 0 && oldSize != TombstoneFileSize {
				nm.DeletionCounter++
				nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
			}
		} else {
			oldSize := nm.m.Delete(Key(key))
			glog.V(3).Infoln("removing key", key, "offset", offset*PaddingSize, "size", size, "oldSize", oldSize)
			nm.DeletionCounter++
			nm.DeletionByteCounter = nm.DeletionByteCounter + uint64(oldSize)
		}
		return nil
	})
	glog.V(1).Infof("max file key: %d for file: %s", nm.MaximumFileKey, file.Name())
	return nm, e
}

func LoadCompactNeedleMap(file *os.File) (*NeedleMap, error) {
	nm := NewCompactNeedleMap(file)
	return doLoading(file, nm)
}

func (nm *baseNeedleMapper) IndexFileSize() uint64 {
	stat, err := nm.indexFile.Stat()
	if err == nil {
		return uint64(stat.Size())
	}
	return 0
}

func (nm *baseNeedleMapper) IndexFileName() string {
	return nm.indexFile.Name()
}

func (nm *baseNeedleMapper) appendToIndexFile(key uint64, offset uint32, size uint32) error {
	bytes := make([]byte, 16)
	util.Uint64toBytes(bytes[0:8], key)
	util.Uint32toBytes(bytes[8:12], offset)
	util.Uint32toBytes(bytes[12:16], size)

	nm.indexFileAccessLock.Lock()
	defer nm.indexFileAccessLock.Unlock()
	if _, err := nm.indexFile.Seek(0, 2); err != nil {
		return fmt.Errorf("cannot seek end of indexfile %s: %v",
			nm.indexFile.Name(), err)
	}
	_, err := nm.indexFile.Write(bytes)
	return err
}

func (nm *baseNeedleMapper) IndexFileContent() ([]byte, error) {
	nm.indexFileAccessLock.Lock()
	defer nm.indexFileAccessLock.Unlock()
	return ioutil.ReadFile(nm.indexFile.Name())
}

func (mm *mapMetric) logDelete(deletedByteCount uint32) {
	mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(deletedByteCount)
	mm.DeletionCounter++
}

func (mm *mapMetric) logPut(key uint64, oldSize uint32, newSize uint32) {
	if key > mm.MaximumFileKey {
		mm.MaximumFileKey = key
	}
	mm.FileCounter++
	mm.FileByteCounter = mm.FileByteCounter + uint64(newSize)
	if oldSize > 0 {
		mm.DeletionCounter++
		mm.DeletionByteCounter = mm.DeletionByteCounter + uint64(oldSize)
	}
}

func (mm mapMetric) ContentSize() uint64 {
	return mm.FileByteCounter
}

func (mm mapMetric) DeletedSize() uint64 {
	return mm.DeletionByteCounter
}

func (mm mapMetric) FileCount() int {
	return mm.FileCounter
}

func (mm mapMetric) DeletedCount() int {
	return mm.DeletionCounter
}

func (mm mapMetric) MaxFileKey() uint64 {
	return mm.MaximumFileKey
}
