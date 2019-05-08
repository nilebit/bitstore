package volume

import (
	"fmt"
	"github.com/nilebit/bitstore/disk/needle"
	"github.com/nilebit/bitstore/disk/version"
	"os"
)

func getFileSize(file *os.File) (size int64, err error) {
	var fi os.FileInfo
	if fi, err = file.Stat(); err == nil {
		size = fi.Size()
	}
	return
}

func verifyIndexFileIntegrity(indexFile *os.File) (indexSize int64, err error) {
	if indexSize, err = getFileSize(indexFile); err == nil {
		if indexSize % needle.IndexSize != 0 {
			err = fmt.Errorf("index file's size is %d bytes, maybe corrupted", indexSize)
		}
	}
	return
}

func readIndexEntryAtOffset(indexFile *os.File, offset int64) (bytes []byte, err error) {
	if offset < 0 {
		err = fmt.Errorf("offset %d for index file is invalid", offset)
		return
	}
	bytes = make([]byte, needle.IndexSize)
	_, err = indexFile.ReadAt(bytes, offset)
	return
}

func verifyNeedleIntegrity(datFile *os.File, v version.Version, offset int64, key uint64, size uint32) error {
	n := new(needle.Needle)
	err := n.ReadData(datFile, offset, size, v)
	if err != nil {
		return err
	}
	if n.Id != key {
		return fmt.Errorf("index key %#x does not match needle's Id %#x", key, n.Id)
	}
	return nil
}

func CheckDataIntegrity(v *Volume, indexFile *os.File) error {
	var indexSize int64
	var e error
	if indexSize, e = verifyIndexFileIntegrity(indexFile); e != nil {
		return fmt.Errorf("verifyIndexFileIntegrity %s failed: %v", indexFile.Name(), e)
	}
	if indexSize == 0 {
		return nil
	}
	var lastIdxEntry []byte
	if lastIdxEntry, e = readIndexEntryAtOffset(indexFile, indexSize-needle.IndexSize); e != nil {
		return fmt.Errorf("readLastIndexEntry %s failed: %v", indexFile.Name(), e)
	}
	key, offset, size := idxFileEntry(lastIdxEntry)
	if offset == 0 || size == TombstoneFileSize {
		return nil
	}
	if e = verifyNeedleIntegrity(v.dataFile, v.Version(), int64(offset)*needle.PaddingSize, key, size); e != nil {
		return fmt.Errorf("verifyNeedleIntegrity %s failed: %v", indexFile.Name(), e)
	}

	return nil
}
