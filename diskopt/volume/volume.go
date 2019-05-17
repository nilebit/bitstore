package volume

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/needle"
	"github.com/nilebit/bitstore/diskopt/replicate"
	"github.com/nilebit/bitstore/diskopt/ttl"
	"github.com/nilebit/bitstore/pb/manage_server_pb"
	"github.com/nilebit/bitstore/util"
)

type Volume struct {
	Id         util.VIDType
	dir        string
	Collection string
	dataFile   *os.File
	NM         needle.Mapper
	ReadOnly   bool

	SuperBlock

	dataFileAccessLock sync.Mutex
	lastModifiedTime   uint64 //unix time in seconds

	lastCompactIndexOffset uint64
	lastCompactRevision    uint16
}

func NewVolume(dirname string, collection string, id util.VIDType,
	replicaPlacement *replicate.Placement,
	ttl *ttl.TTL, preallocate int64) (v *Volume, e error) {

	v = &Volume{dir: dirname, Collection: collection, Id: id}
	v.SuperBlock = SuperBlock{ReplicaPlacement: replicaPlacement, Ttl: ttl}
	e = v.load(true, true, preallocate)
	return
}

func VolumeFileName(collection string, dir string, id int) (fileName string) {
	idString := strconv.Itoa(id)
	if collection == "" {
		fileName = path.Join(dir, idString)
	} else {
		fileName = path.Join(dir, collection+"_"+idString)
	}
	return
}

func (v *Volume) FileName() (fileName string) {
	return VolumeFileName(v.Collection, v.dir, int(v.Id))
}

func checkFile(filename string) (exists, canRead, canWrite bool, modTime time.Time, fileSize int64) {
	exists = true
	fi, err := os.Stat(filename)
	if os.IsNotExist(err) {
		exists = false
		return
	}
	if fi.Mode()&0400 != 0 {
		canRead = true
	}
	if fi.Mode()&0200 != 0 {
		canWrite = true
	}
	modTime = fi.ModTime()
	fileSize = fi.Size()
	return
}

func createVolumeFile(fileName string, preallocate int64) (file *os.File, e error) {
	file, e = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0644)
	if preallocate > 0 {
		glog.V(0).Infof("Preallocated disk space for %s is not supported", fileName)
	}
	return file, e
}

func (v *Volume) Size() int64 {
	stat, e := v.dataFile.Stat()
	if e == nil {
		return stat.Size()
	}
	glog.V(0).Infof("Failed to read file size %s %v", v.dataFile.Name(), e)
	return -1
}

func (v *Volume) load(alsoLoadIndex bool, createDatIfMissing bool, preallocate int64) error {
	var e error
	fileName := VolumeFileName(v.Collection, v.dir, int(v.Id))
	alreadyHasSuperBlock := false
	exists, canRead, canWrite, modifiedTime, fileSize := checkFile(fileName + ".dat")
	if exists {
		if !canRead {
			return fmt.Errorf("cannot read Volume Data file %s.dat", fileName)
		}
		if canWrite {
			v.dataFile, e = os.OpenFile(fileName+".dat", os.O_RDWR|os.O_CREATE, 0644)
			v.lastModifiedTime = uint64(modifiedTime.Unix())
		} else {
			glog.V(0).Infoln("opening " + fileName + ".dat in READONLY mode")
			v.dataFile, e = os.Open(fileName + ".dat")
			v.ReadOnly = true
		}
		if fileSize >= SuperBlockSize {
			alreadyHasSuperBlock = true
		}
	} else {
		if createDatIfMissing {
			v.dataFile, e = createVolumeFile(fileName+".dat", preallocate)
		} else {
			return fmt.Errorf("Volume Data file %s.dat does not exist.", fileName)
		}
	}

	if e != nil {
		if !os.IsPermission(e) {
			return fmt.Errorf("cannot load Volume Data %s.dat: %v", fileName, e)
		} else {
			return fmt.Errorf("load data file %s.dat: %v", fileName, e)
		}
	}

	if alreadyHasSuperBlock {
		e = v.readSuperBlock()
	} else {
		e = v.maybeWriteSuperBlock()
	}

	if e == nil && alsoLoadIndex {
		var indexFile *os.File
		if v.ReadOnly {
			glog.V(1).Infoln("open to read file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDONLY, 0644); e != nil {
				return fmt.Errorf("cannot read Volume Index %s.idx: %v", fileName, e)
			}
		} else {
			glog.V(1).Infoln("open to write file", fileName+".idx")
			if indexFile, e = os.OpenFile(fileName+".idx", os.O_RDWR|os.O_CREATE, 0644); e != nil {
				return fmt.Errorf("cannot write Volume Index %s.idx: %v", fileName, e)
			}
		}

		if e = CheckDataIntegrity(v, indexFile); e != nil {
			v.ReadOnly = true
			glog.V(0).Infof("volumeDataIntegrityChecking failed %v", e)
		}

		glog.V(0).Infoln("loading index", fileName+".idx", "to memory readonly", v.ReadOnly)
		if v.NM, e = needle.LoadCompactNeedleMap(indexFile); e != nil {
			glog.V(0).Infof("loading index %s to memory error: %v", fileName+".idx", e)
		}

	}

	return e
}

func (v *Volume) ContentSize() uint64 {
	return v.NM.ContentSize()
}

type FileId struct {
	VolumeId util.VIDType
	Key      uint64
	Hashcode uint32
}

func NewFileIdFromNeedle(VolumeId util.VIDType, n *needle.Needle) *FileId {
	return &FileId{VolumeId: VolumeId, Key: n.Id, Hashcode: n.Cookie}
}

func (n *FileId) String() string {
	bytes := make([]byte, 12)
	util.Uint64toBytes(bytes[0:8], n.Key)
	util.Uint32toBytes(bytes[8:12], n.Hashcode)
	nonzeroIndex := 0
	for ; bytes[nonzeroIndex] == 0; nonzeroIndex++ {
	}
	return n.VolumeId.String() + "," + hex.EncodeToString(bytes[nonzeroIndex:])
}

// isFileUnchanged checks whether this needle to write is same as last one.
// It requires serialized access in the same volume.
func (v *Volume) isFileUnchanged(n *needle.Needle) bool {
	if v.Ttl.String() != "" {
		return false
	}
	nv, ok := v.NM.Get(n.Id)
	if ok && nv.Offset > 0 {
		oldNeedle := new(needle.Needle)
		err := oldNeedle.ReadData(v.dataFile, int64(nv.Offset)*needle.PaddingSize, nv.Size, v.Version())
		if err != nil {
			glog.V(0).Infof("Failed to check updated file %v", err)
			return false
		}
		defer oldNeedle.ReleaseMemory()
		if oldNeedle.Checksum == n.Checksum && bytes.Equal(oldNeedle.Data, n.Data) {
			n.DataSize = oldNeedle.DataSize
			return true
		}
	}
	return false
}

func (v *Volume) WriteNeedle(n *needle.Needle) (size uint32, isUnchanged bool, err error) {
	glog.V(4).Infof("writing needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.ReadOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if v.isFileUnchanged(n) {
		size = n.DataSize
		glog.V(4).Infof("needle is unchanged!")
		isUnchanged = true
		return
	}

	var offset int64
	if offset, err = v.dataFile.Seek(0, 2); err != nil {
		glog.V(0).Infof("failed to seek the end of file: %v", err)
		return
	}

	//ensure file writing starting from aligned positions
	if offset%needle.PaddingSize != 0 {
		offset = offset + (needle.PaddingSize - offset%needle.PaddingSize)
		if offset, err = v.dataFile.Seek(offset, 0); err != nil {
			glog.V(0).Infof("failed to align in datafile %s: %v", v.dataFile.Name(), err)
			return
		}
	}

	if size, _, err = n.Append(v.dataFile, v.Version()); err != nil {
		if e := v.dataFile.Truncate(offset); e != nil {
			err = fmt.Errorf("%s\ncannot truncate %s: %v", err, v.dataFile.Name(), e)
		}
		return
	}

	nv, ok := v.NM.Get(n.Id)
	if !ok || int64(nv.Offset)*needle.PaddingSize < offset {
		if err = v.NM.Put(n.Id, uint32(offset/needle.PaddingSize), n.Size); err != nil {
			glog.V(4).Infof("failed to save in needle map %d: %v", n.Id, err)
		}
	}
	if v.lastModifiedTime < n.LastModified {
		v.lastModifiedTime = n.LastModified
	}
	return
}

func (v *Volume) NeedToReplicate() bool {
	return v.ReplicaPlacement.GetCopyCount() > 1
}

// read fills in Needle content by looking up n.Id from NeedleMapper
func (v *Volume) ReadNeedle(n *needle.Needle) (int, error) {
	nv, ok := v.NM.Get(n.Id)
	if !ok || nv.Offset == 0 {
		return -1, errors.New("Not Found")
	}
	if nv.Size == needle.TombstoneFileSize {
		return -1, errors.New("Already Deleted")
	}
	err := n.ReadData(v.dataFile, int64(nv.Offset)*needle.PaddingSize, nv.Size, v.Version())
	if err != nil {
		return 0, err
	}
	bytesRead := len(n.Data)
	if !n.HasTtl() {
		return bytesRead, nil
	}
	ttlMinutes := n.Ttl.Minutes()
	if ttlMinutes == 0 {
		return bytesRead, nil
	}
	if !n.HasLastModifiedDate() {
		return bytesRead, nil
	}
	if uint64(time.Now().Unix()) < n.LastModified+uint64(ttlMinutes*60) {
		return bytesRead, nil
	}
	n.ReleaseMemory()
	return -1, errors.New("Not Found")
}

func (v *Volume) DeleteNeedle(n *needle.Needle) (uint32, error) {
	glog.V(4).Infof("delete needle %s", NewFileIdFromNeedle(v.Id, n).String())
	if v.ReadOnly {
		return 0, fmt.Errorf("%s is read-only", v.dataFile.Name())
	}
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	nv, ok := v.NM.Get(n.Id)
	//fmt.Println("key", n.Id, "volume offset", nv.Offset, "data_size", n.Size, "cached size", nv.Size)
	if ok && nv.Size != needle.TombstoneFileSize {
		size := nv.Size
		offset, err := v.dataFile.Seek(0, 2)
		if err != nil {
			return size, err
		}
		if err := v.NM.Delete(n.Id, uint32(offset/needle.PaddingSize)); err != nil {
			return size, err
		}
		n.Data = nil
		_, _, err = n.Append(v.dataFile, v.Version())
		return size, err
	}
	return 0, nil
}

func (v *Volume) FileStat() (datSize uint64, idxSize uint64, modTime time.Time) {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()

	if v.dataFile == nil {
		return
	}

	stat, e := v.dataFile.Stat()
	if e == nil {
		return uint64(stat.Size()), v.NM.IndexFileSize(), stat.ModTime()
	}
	glog.V(0).Infof("Failed to read file size %s %v", v.dataFile.Name(), e)
	return // -1 causes integer overflow and the volume to become unwritable.
}

func (v *Volume) ToVolumeInformationMessage() *manage_server_pb.VolumeInformationMessage {
	size, _, _ := v.FileStat()
	return &manage_server_pb.VolumeInformationMessage{
		Id:               uint32(v.Id),
		Size:             size,
		Collection:       v.Collection,
		FileCount:        uint64(v.NM.FileCount()),
		DeleteCount:      uint64(v.NM.DeletedCount()),
		DeletedByteCount: v.NM.DeletedSize(),
		ReadOnly:         v.ReadOnly,
		ReplicaPlacement: uint32(v.ReplicaPlacement.Byte()),
		Version:          uint32(v.Version()),
		Ttl:              v.Ttl.ToUint32(),
	}
}

func (v *Volume) Expired(volumeSizeLimit uint64) bool {
	if volumeSizeLimit == 0 {
		//skip if we don't know size limit
		return false
	}
	if v.ContentSize() == 0 {
		return false
	}
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	glog.V(1).Infof("now:%v lastModified:%v", time.Now().Unix(), v.lastModifiedTime)
	livedMinutes := (time.Now().Unix() - int64(v.lastModifiedTime)) / 60
	glog.V(1).Infof("ttl:%v lived:%v", v.Ttl, livedMinutes)
	if int64(v.Ttl.Minutes()) < livedMinutes {
		return true
	}
	return false
}

// wait either maxDelayMinutes or 10% of ttl minutes
func (v *Volume) ExiredLongEnough(maxDelayMinutes uint32) bool {
	if v.Ttl == nil || v.Ttl.Minutes() == 0 {
		return false
	}
	removalDelay := v.Ttl.Minutes() / 10
	if removalDelay > maxDelayMinutes {
		removalDelay = maxDelayMinutes
	}

	if uint64(v.Ttl.Minutes()+removalDelay)*60+v.lastModifiedTime < uint64(time.Now().Unix()) {
		return true
	}
	return false
}

// Close cleanly shuts down this volume
func (v *Volume) Close() {
	v.dataFileAccessLock.Lock()
	defer v.dataFileAccessLock.Unlock()
	v.NM.Close()
	_ = v.dataFile.Close()
}

// Destroy removes everything related to this volume
func (v *Volume) Destroy() (err error) {
	if v.ReadOnly {
		err = fmt.Errorf("%s is read-only", v.dataFile.Name())
		return
	}
	v.Close()
	os.Remove(v.FileName() + ".dat")
	os.Remove(v.FileName() + ".idx")
	os.Remove(v.FileName() + ".cpd")
	os.Remove(v.FileName() + ".cpx")
	return
}
