package util

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/golang/glog"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

var (
	client    *http.Client
	Transport *http.Transport
)

type VIDType uint32

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

func Post(url string, values url.Values) ([]byte, error) {
	r, err := client.PostForm(url, values)
	if err != nil {
		return nil, err
	}
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if r.StatusCode >= 400 {
		if err != nil {
			return nil, fmt.Errorf("%s: %d - %s", url, r.StatusCode, string(b))
		} else {
			return nil, fmt.Errorf("%s: %s", url, r.Status)
		}
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}


/*
* Default more not to gzip since gzip can be done on client side.
 */func IsGzippableFileType(ext, mtype string) (shouldBeZipped, iAmSure bool) {

	// text
	if strings.HasPrefix(mtype, "text/") {
		return true, true
	}

	// images
	switch ext {
	case ".svg", ".bmp":
		return true, true
	}
	if strings.HasPrefix(mtype, "image/") {
		return false, true
	}

	// by file name extension
	switch ext {
	case ".zip", ".rar", ".gz", ".bz2", ".xz":
		return false, true
	case ".pdf", ".txt", ".html", ".htm", ".css", ".js", ".json":
		return true, true
	case ".php", ".java", ".go", ".rb", ".c", ".cpp", ".h", ".hpp":
		return true, true
	case ".png", ".jpg", ".jpeg":
		return false, true
	}

	// by mime type
	if strings.HasPrefix(mtype, "application/") {
		if strings.HasSuffix(mtype, "xml") {
			return true, true
		}
		if strings.HasSuffix(mtype, "script") {
			return true, true
		}
	}

	return false, false
}

func NormalizeUrl(url string) string {
	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		return url
	}
	return "http://" + url
}

func UnGzipData(input []byte) ([]byte, error) {
	buf := bytes.NewBuffer(input)
	r, _ := gzip.NewReader(buf)
	defer r.Close()
	output, err := ioutil.ReadAll(r)
	if err != nil {
		glog.V(2).Infoln("error uncompressing data:", err)
	}
	return output, err
}
