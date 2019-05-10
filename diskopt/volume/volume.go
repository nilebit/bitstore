package volume

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/needle"
	"github.com/nilebit/bitstore/diskopt/replica"
	"github.com/nilebit/bitstore/diskopt/ttl"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

type VIDType uint32

type Volume struct {
	Id            VIDType
	dir           string
	Collection    string
	dataFile      *os.File
	nm            needle.Mapper
	readOnly      bool

	SuperBlock

	dataFileAccessLock sync.Mutex
	lastModifiedTime   uint64 //unix time in seconds

	lastCompactIndexOffset uint64
	lastCompactRevision    uint16
}

func NewVolumeId(vid string) (VIDType, error) {
	volumeId, err := strconv.ParseUint(vid, 10, 64)
	return VIDType(volumeId), err
}

func (vid *VIDType) String() string {
	return strconv.FormatUint(uint64(*vid), 10)
}

func (vid *VIDType) Next() VIDType {
	return VIDType(uint32(*vid) + 1)
}


func NewVolume(dirname string, collection string, id VIDType,
	replicaPlacement *replica.Placement,
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
			v.readOnly = true
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
		if v.readOnly {
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
			v.readOnly = true
			glog.V(0).Infof("volumeDataIntegrityChecking failed %v", e)
		}

		glog.V(0).Infoln("loading index", fileName+".idx", "to memory readonly", v.readOnly)
		if v.nm, e = needle.LoadCompactNeedleMap(indexFile); e != nil {
			glog.V(0).Infof("loading index %s to memory error: %v", fileName+".idx", e)
		}

	}

	return e
}