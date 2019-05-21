package diskserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/nilebit/bitstore/diskopt/volume"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/needle"
	"github.com/nilebit/bitstore/util"
)

type UploadResult struct {
	Name  string `json:"name,omitempty"`
	Size  uint32 `json:"size,omitempty"`
	Error string `json:"error,omitempty"`
	ETag  string `json:"eTag,omitempty"`
}

func (s *DiskServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	ctx := NewContext(r)
	defer Summary(w, ctx)

	stat := make(map[string]interface{})
	stat["Version"] = util.VERSION
	stat["Volumes"] = s.Disk.Status()

	stat["cpu"] = runtime.NumCPU()
	stat["goroutine"] = runtime.NumGoroutine()
	stat["cgocall"] = runtime.NumCgoCall()
	gcStat := &debug.GCStats{}
	debug.ReadGCStats(gcStat)
	stat["gc"] = gcStat.NumGC
	stat["pausetotal"] = gcStat.PauseTotal.Nanoseconds()

	ctx.RespHttpStatus = http.StatusOK
	ctx.RespBodyJson = stat
}

func parseURLPath(path string) (vid, fid, filename, ext string, isVIdOnly bool) {
	switch strings.Count(path, "/") {
	case 3:
		parts := strings.Split(path, "/")
		vid, fid, filename = parts[1], parts[2], parts[3]
		ext = filepath.Ext(filename)
	case 2:
		parts := strings.Split(path, "/")
		vid, fid = parts[1], parts[2]
		dotIndex := strings.LastIndex(fid, ".")
		if dotIndex > 0 {
			ext = fid[dotIndex:]
			fid = fid[0:dotIndex]
		}
	default:
		sepIndex := strings.LastIndex(path, "/")
		commaIndex := strings.LastIndex(path[sepIndex:], ",")
		if commaIndex <= 0 {
			vid, isVIdOnly = path[sepIndex+1:], true
			return
		}
		dotIndex := strings.LastIndex(path[sepIndex:], ".")
		vid = path[sepIndex+1 : commaIndex]
		fid = path[commaIndex+1:]
		ext = ""
		if dotIndex > 0 {
			fid = path[commaIndex+1 : dotIndex]
			ext = path[dotIndex:]
		}
	}
	return
}

func (d *DiskServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	if e := r.ParseForm(); e != nil {
		glog.V(0).Infoln("form parse error:", e)
		writeJsonError(w, r, http.StatusBadRequest, e)
		return
	}
	vid, _, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, ve := util.NewVolumeId(vid)
	if ve != nil {
		glog.V(0).Infoln("NewVolumeId error:", ve)
		writeJsonError(w, r, http.StatusBadRequest, ve)
		return
	}
	needle, originalSize, ne := needle.NewNeedle(r)
	if ne != nil {
		writeJsonError(w, r, http.StatusBadRequest, ne)
		return
	}

	ret := UploadResult{}

	// 写入数据并同步多备份
	_, isUnchanged, writeError := d.ReplicatedWrite(d.CurrentLeader, volumeId, needle, r)
	httpStatus := http.StatusCreated
	if isUnchanged {
		httpStatus = http.StatusNotModified
	}
	if writeError != nil {
		httpStatus = http.StatusInternalServerError
		ret.Error = writeError.Error()
	}
	if needle.HasName() {
		ret.Name = string(needle.Name)
	}
	ret.Size = uint32(originalSize)
	etag := needle.Etag()
	w.Header().Set("Etag", etag)

	if err := writeJson(w, r, httpStatus, ret); err != nil {
		glog.V(0).Infof("error writing JSON %v: %v", ret, err)
	}

	return
}

func (d *DiskServer) GetHandler(w http.ResponseWriter, r *http.Request) {
	vid, fid, filename, ext, _ := parseURLPath(r.URL.Path)
	volumeId, err := util.NewVolumeId(vid)
	if err != nil {
		glog.V(2).Infoln("parsing error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	n := new(needle.Needle)
	err = n.ParsePath(fid)
	if err != nil {
		glog.V(2).Infoln("parsing fid error:", err, r.URL.Path)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	glog.V(4).Infoln("volume", volumeId, "reading", n)
	if !d.Disk.HasVolume(volumeId) {
		lookupResult, err := Lookup(d.CurrentLeader, volumeId.String())
		glog.V(2).Infoln("volume", volumeId, "found on", lookupResult, "error", err)
		if err == nil && len(lookupResult.Locations) > 0 {
			u, _ := url.Parse(util.NormalizeUrl(lookupResult.Locations[0].Url))
			u.Path = r.URL.Path
			arg := url.Values{}
			if c := r.FormValue("collection"); c != "" {
				arg.Set("collection", c)
			}
			u.RawQuery = arg.Encode()
			http.Redirect(w, r, u.String(), http.StatusMovedPermanently)

		} else {
			glog.V(2).Infoln("lookup error:", err, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
		return
	}

	cookie := n.Cookie
	count, e := d.Disk.ReadVolumeNeedle(volumeId, n)
	glog.V(4).Infoln("read bytes", count, "error", e)
	if e != nil || count < 0 {
		glog.V(0).Infoln("read error:", e, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	defer n.ReleaseMemory()
	if n.Cookie != cookie {
		glog.V(0).Infoln("request", r.URL.Path, "with unmaching cookie seen:", cookie, "expected:", n.Cookie, "from", r.RemoteAddr, "agent", r.UserAgent())
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if n.LastModified != 0 {
		w.Header().Set("Last-Modified", time.Unix(int64(n.LastModified), 0).UTC().Format(http.TimeFormat))
		if r.Header.Get("If-Modified-Since") != "" {
			if t, parseError := time.Parse(http.TimeFormat, r.Header.Get("If-Modified-Since")); parseError == nil {
				if t.Unix() >= int64(n.LastModified) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}
	etag := n.Etag()
	if inm := r.Header.Get("If-None-Match"); inm == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Etag", etag)

	if n.HasPairs() {
		pairMap := make(map[string]string)
		err = json.Unmarshal(n.Pairs, &pairMap)
		if err != nil {
			glog.V(0).Infoln("Unmarshal pairs error:", err)
		}
		for k, v := range pairMap {
			w.Header().Set(k, v)
		}
	}

	if n.NameSize > 0 && filename == "" {
		filename = string(n.Name)
		if ext == "" {
			ext = path.Ext(filename)
		}
	}
	mtype := ""
	if n.MimeSize > 0 {
		mt := string(n.Mime)
		if !strings.HasPrefix(mt, "application/octet-stream") {
			mtype = mt
		}
	}

	if ext != ".gz" {
		if n.IsGzipped() {
			if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
				w.Header().Set("Content-Encoding", "gzip")
			} else {
				if n.Data, err = util.UnGzipData(n.Data); err != nil {
					glog.V(0).Infoln("ungzip error:", err, r.URL.Path)
				}
			}
		}
	}

	if e := writeResponseContent(filename, mtype, bytes.NewReader(n.Data), w, r); e != nil {
		glog.V(2).Infoln("response write error:", e)
	}

	return
}

func (d *DiskServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	n := new(needle.Needle)
	vid, fid, _, _, _ := parseURLPath(r.URL.Path)
	volumeId, _ := util.NewVolumeId(vid)
	n.ParsePath(fid)

	cookie := n.Cookie

	_, ok := d.Disk.ReadVolumeNeedle(volumeId, n)
	if ok != nil {
		m := make(map[string]uint32)
		m["size"] = 0
		if err := writeJson(w, r, http.StatusNotFound, m); err != nil {
			glog.V(0).Infof("error writing JSON %+v status %d: %v", m, http.StatusNotFound, err)
		}
		return
	}

	if n.Cookie != cookie {
		glog.V(0).Infoln("delete", r.URL.Path, "with unmaching cookie from ", r.RemoteAddr, "agent", r.UserAgent())
		writeJsonError(w, r, http.StatusBadRequest, errors.New("File Random Cookie does not match."))
		return
	}

	count := int64(n.Size)

	n.LastModified = uint64(time.Now().Unix())
	if len(r.FormValue("ts")) > 0 {
		modifiedTime, err := strconv.ParseInt(r.FormValue("ts"), 10, 64)
		if err == nil {
			n.LastModified = uint64(modifiedTime)
		}
	}

	_, err := d.ReplicatedDelete(d.CurrentLeader, volumeId, n, r)

	if err == nil {
		m := make(map[string]int64)
		m["size"] = count
		if err := writeJson(w, r, http.StatusAccepted, m); err != nil {
			glog.V(0).Infof("error writing JSON %+v status %d: %v", m, http.StatusNotFound, err)
		}
	} else {
		writeJsonError(w, r, http.StatusInternalServerError, fmt.Errorf("Deletion Failed: %v", err))
	}
}

func (d *DiskServer) AssignVolumeHandler(w http.ResponseWriter, r *http.Request) {
	ctx := NewContext(r)
	defer Summary(w, ctx)

	var err error
	preallocate := int64(0)
	preallocateStr := r.FormValue("preallocate")

	if preallocateStr != "" {
		preallocate, err = strconv.ParseInt(preallocateStr, 10, 64)
		if err != nil {
			glog.V(0).Infof("ignoring invalid int64 value for preallocate = %s \n", preallocateStr)
		}
	}
	err = d.Disk.AddVolume(
		r.FormValue("volume"),
		r.FormValue("collection"),
		r.FormValue("replication"),
		r.FormValue("ttl"),
		preallocate,
	)

	if err == nil {
		ctx.RespBodyJson = map[string]string{"error": ""}
		ctx.RespHttpStatus = http.StatusAccepted
	} else {
		ctx.Err = err
		ctx.RespHttpStatus = http.StatusNotAcceptable
	}
	return
}

func (d *DiskServer) VolumeDeleteHandler(w http.ResponseWriter, r *http.Request) {
	ctx := NewContext(r)
	defer Summary(w, ctx)

	volumeIdString := r.FormValue("volume")
	if volumeIdString == "" {
		ctx.RespHttpStatus = http.StatusNotFound
		ctx.Err = fmt.Errorf("Empty Volume Id: Need to pass in volume.")
		return
	}

	vid, err := volume.NewVolumeId(volumeIdString)
	if err != nil {
		ctx.RespHttpStatus = http.StatusNotFound
		ctx.Err = fmt.Errorf("Volume Id %s is not a valid unsigned integer, %s", volumeIdString, err.Error())
		return
	}

	err = d.Disk.DeleteVolume(vid)
	if err != nil {
		ctx.RespHttpStatus = http.StatusInternalServerError
		ctx.Err = err
		return
	}
	ctx.RespHttpStatus = http.StatusOK
	ctx.RespBodyJson = "Volume deleted"
	return
}

func (d *DiskServer) VacuumVolumeCheckHandler(w http.ResponseWriter, r *http.Request) {
	ctx := NewContext(r)
	defer Summary(w, ctx)

	err, ret := d.Disk.CheckCompactVolume(r.FormValue("volume"), r.FormValue("garbageThreshold"))
	if err != nil {
		ctx.Err = err
		ctx.RespHttpStatus = http.StatusInternalServerError
		return
	}

	ctx.RespBodyJson = map[string]interface{}{"error": "", "result": ret}
	ctx.RespHttpStatus = http.StatusOK
}

func (d *DiskServer) VacuumVolumeCompactHandler(w http.ResponseWriter, r *http.Request) {
	ctx := NewContext(r)
	defer Summary(w, ctx)

	err := d.Disk.CompactVolume(r.FormValue("volume"))
	if err != nil {
		ctx.RespHttpStatus = http.StatusInternalServerError
		ctx.Err = err
		return
	}

	ctx.RespHttpStatus = http.StatusOK
	ctx.RespBodyJson = map[string]string{"error": ""}
}

func (d *DiskServer) VacuumVolumeCommitHandler(w http.ResponseWriter, r *http.Request) {
	ctx := NewContext(r)
	defer Summary(w, ctx)

	err := d.Disk.CommitCompactVolume(r.FormValue("volume"))
	if err != nil {
		ctx.RespHttpStatus = http.StatusInternalServerError
		ctx.Err = err
		return
	}

	ctx.RespHttpStatus = http.StatusOK
	ctx.RespBodyJson = map[string]string{"error": ""}
}
