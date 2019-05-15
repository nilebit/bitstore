package diskserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/needle"
	"github.com/nilebit/bitstore/security"
	"github.com/nilebit/bitstore/util"
	"io"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Location struct {
	Url       string `json:"url,omitempty"`
}

type LookupResult struct {
	VolumeId  string     `json:"volumeId,omitempty"`
	Locations []Location `json:"locations,omitempty"`
	Error     string     `json:"error,omitempty"`
}


type VidInfo struct {
	Locations       []Location
	NextRefreshTime time.Time
}

type VidCache struct {
	sync.RWMutex
	cache []VidInfo
}

var (
	ErrorNotFound = errors.New("not found")
	vc VidCache
)

func (vc *VidCache) Get(vid string) ([]Location, error) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return nil, err
	}
	vc.RLock()
	defer vc.RUnlock()
	if 0 < id && id <= len(vc.cache) {
		if vc.cache[id-1].Locations == nil {
			return nil, errors.New("not set")
		}
		if vc.cache[id-1].NextRefreshTime.Before(time.Now()) {
			return nil, errors.New("expired")
		}
		return vc.cache[id-1].Locations, nil
	}
	return nil, ErrorNotFound
}

func (vc *VidCache) Set(vid string, locations []Location, duration time.Duration) {
	id, err := strconv.Atoi(vid)
	if err != nil {
		glog.V(1).Infof("Unknown volume id %s", vid)
		return
	}
	vc.Lock()
	defer vc.Unlock()
	if id > len(vc.cache) {
		for i := id - len(vc.cache); i > 0; i-- {
			vc.cache = append(vc.cache, VidInfo{})
		}
	}
	if id > 0 {
		vc.cache[id-1].Locations = locations
		vc.cache[id-1].NextRefreshTime = time.Now().Add(duration)
	}
}

func doLookup(server string, vid string) (*LookupResult, error) {
	values := make(url.Values)
	values.Add("volumeId", vid)
	jsonBlob, err := util.Post("http://"+server+"/dir/lookup", values)
	if err != nil {
		return nil, err
	}
	var ret LookupResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return nil, err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}

func Lookup(server string, vid string) (ret *LookupResult, err error) {
	locations, cache_err := vc.Get(vid)
	if cache_err != nil {
		if ret, err = doLookup(server, vid); err == nil {
			vc.Set(vid, ret.Locations, 10*time.Minute)
		}
	} else {
		ret = &LookupResult{VolumeId: vid, Locations: locations}
	}
	return
}

type RemoteResult struct {
	Host  string
	Error error
}

type DistributedOperationResult map[string]error
func (dr DistributedOperationResult) Error() error {
	var errs []string
	for k, v := range dr {
		if v != nil {
			errs = append(errs, fmt.Sprintf("[%s]: %v", k, v))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	return errors.New(strings.Join(errs, "\n"))
}

func (s *DiskServer)ReplicatedWrite(masterNode string, volumeId util.VIDType, n *needle.Needle,
	r *http.Request) (size uint32, isUnchanged bool, errorStatus error) {
	// 检查JWT
	jwt := security.GetJwt(r)

	// 写入数据
	size, isUnchanged, err := s.Disks.Write(volumeId, n)
	if err != nil {
		errorStatus = fmt.Errorf("failed to write to local disk: %v", err)
		return
	}

	needToReplicate := !s.Disks.HasVolume(volumeId)
	needToReplicate = needToReplicate || s.Disks.FindVolume(volumeId).NeedToReplicate()
	if !needToReplicate {
		needToReplicate = s.Disks.FindVolume(volumeId).NeedToReplicate()
	}

	if !needToReplicate || r.FormValue("type") == "replicate" {
		return
	}
	// 分布写入数据
	if err = s.DistributedOperation(masterNode, volumeId,
		func(location Location) error {
			u := url.URL{Scheme: "http", Host: location.Url, Path: r.URL.Path,}
			q := url.Values{"type": {"replicate"},}

			if n.LastModified > 0 {
				q.Set("ts", strconv.FormatUint(n.LastModified, 10))
			}

			u.RawQuery = q.Encode()

			pairMap := make(map[string]string)
			if n.HasPairs() {
				tmpMap := make(map[string]string)
				err := json.Unmarshal(n.Pairs, &tmpMap)
				if err != nil {
					glog.V(0).Infoln("Unmarshal pairs error:", err)
				}
				for k, v := range tmpMap {
					pairMap[needle.PairNamePrefix+k] = v
				}
			}

			_, err := Upload(u.String(), string(n.Name), bytes.NewReader(n.Data), n.IsGzipped(), string(n.Mime), pairMap, jwt)
			return err
		}); err != nil {
		size = 0
		errorStatus = fmt.Errorf("Failed to write to replicas for volume %d: %v", volumeId, err)
	}

	return
}

func (s *DiskServer)DistributedOperation(masterNode string, volumeId util.VIDType, op func(location Location) error) error {
	var d = s.Disks
	if lookupResult, lookupErr := Lookup(masterNode, volumeId.String()); lookupErr == nil {
		length := 0
		selfUrl := (d.Ip + ":" + strconv.Itoa(d.Port))
		results := make(chan RemoteResult)
		for _, location := range lookupResult.Locations {
			if location.Url != selfUrl {
				length++
				go func(location Location, results chan RemoteResult) {
					results <- RemoteResult{location.Url, op(location)}
				}(location, results)
			}
		}
		ret := DistributedOperationResult(make(map[string]error))
		for i := 0; i < length; i++ {
			result := <-results
			ret[result.Host] = result.Error
		}
		if volume :=d.FindVolume(volumeId); volume != nil {
			if length+1 < volume.ReplicaPlacement.GetCopyCount() {
				return fmt.Errorf("replicating opetations [%d] is less than volume's replication copy count [%d]",
					length+1, volume.ReplicaPlacement.GetCopyCount())
			}
		}
		return ret.Error()
	} else {
		glog.V(0).Infoln()
		return fmt.Errorf("Failed to lookup for %d: %v", volumeId, lookupErr)
	}
}

func Upload(uploadUrl string, filename string, reader io.Reader, isGzipped bool, mtype string,
	pairMap map[string]string, jwt security.EncodedJwt) (*UploadResult, error) {
	return uploadContent(uploadUrl, func(w io.Writer) (err error) {
		_, err = io.Copy(w, reader)
		return
	}, filename, isGzipped, mtype, pairMap, jwt)
}

var (
	client *http.Client
)

func init() {
	client = &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 1024,}}
}

func uploadContent(uploadUrl string, fillBufferFunction func(w io.Writer) error, filename string,
	isGzipped bool, mtype string, pairMap map[string]string, jwt security.EncodedJwt) (*UploadResult, error) {
	var fileNameEscaper = strings.NewReplacer("\\", "\\\\", "\"", "\\\"")
	bodyBuf := bytes.NewBufferString("")
	bodyWriter := multipart.NewWriter(bodyBuf)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename="%s"`, fileNameEscaper.Replace(filename)))
	if mtype == "" {
		mtype = mime.TypeByExtension(strings.ToLower(filepath.Ext(filename)))
	}
	if mtype != "" {
		h.Set("Content-Type", mtype)
	}
	if isGzipped {
		h.Set("Content-Encoding", "gzip")
	}
	if jwt != "" {
		h.Set("Authorization", "BEARER "+string(jwt))
	}

	file_writer, cp_err := bodyWriter.CreatePart(h)
	if cp_err != nil {
		glog.V(0).Infoln("error creating form file", cp_err.Error())
		return nil, cp_err
	}
	if err := fillBufferFunction(file_writer); err != nil {
		glog.V(0).Infoln("error copying data", err)
		return nil, err
	}
	content_type := bodyWriter.FormDataContentType()
	if err := bodyWriter.Close(); err != nil {
		glog.V(0).Infoln("error closing body", err)
		return nil, err
	}

	req, postErr := http.NewRequest("POST", uploadUrl, bodyBuf)
	if postErr != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, postErr.Error())
		return nil, postErr
	}
	req.Header.Set("Content-Type", content_type)
	for k, v := range pairMap {
		req.Header.Set(k, v)
	}
	resp, post_err := client.Do(req)
	if post_err != nil {
		glog.V(0).Infoln("failing to upload to", uploadUrl, post_err.Error())
		return nil, post_err
	}
	defer resp.Body.Close()
	resp_body, ra_err := ioutil.ReadAll(resp.Body)
	if ra_err != nil {
		return nil, ra_err
	}
	var ret UploadResult
	unmarshal_err := json.Unmarshal(resp_body, &ret)
	if unmarshal_err != nil {
		glog.V(0).Infoln("failing to read upload response", uploadUrl, string(resp_body))
		return nil, unmarshal_err
	}
	if ret.Error != "" {
		return nil, errors.New(ret.Error)
	}
	return &ret, nil
}
