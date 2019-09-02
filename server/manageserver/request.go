package manageserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/replicate"
	"github.com/nilebit/bitstore/diskopt/ttl"
	"github.com/nilebit/bitstore/diskopt/version"
	"github.com/nilebit/bitstore/diskopt/volume"
	"github.com/nilebit/bitstore/util"
	"github.com/prometheus/common/model"
	"net/http"
	"net/url"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/nilebit/bitstore/pb"
)

var (
	MaxVolumeID = "max_volume_id"
)

// StatusHandler Server status
func (s *ManageServer) StatusHandler(w http.ResponseWriter, r *http.Request) {
	stat := make(map[string]interface{})
	stat["cpu"] = runtime.NumCPU()
	stat["goroutine"] = runtime.NumGoroutine()
	stat["cgocall"] = runtime.NumCgoCall()
	gcStat := &debug.GCStats{}
	debug.ReadGCStats(gcStat)
	stat["gc"] = gcStat.NumGC
	stat["pausetotal"] = gcStat.PauseTotal.Nanoseconds()

	bytes, err := json.Marshal(stat)
	if err != nil {
		bytes = []byte("json marshal error")
	}
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(bytes)
	return
}

func (s *ManageServer) updateStore(hb *pb.Heartbeat) bool {
	s.IDCMtx.Lock()
	defer s.IDCMtx.Unlock()
	var center *DataCenter
	var rack *DataRack
	var node *DataNode
	if hb.DataCenter == "" {
		hb.DataCenter = DefaultCenterName
	}

	_, has := s.IDC.DataCenters[hb.DataCenter]
	if has == false {
		s.IDC.DataCenters[hb.DataCenter] = NewDataCenter()
	}
	center = s.IDC.DataCenters[hb.DataCenter]

	if hb.Rack == "" {
		hb.Rack = DefaultRackName
	}
	_, has = center.DataRack[hb.Rack]
	if has == false {
		center.DataRack[hb.Rack] = NewRack()
	}
	rack = center.DataRack[hb.Rack]

	diskId := hb.Ip + ":" + strconv.Itoa(int(hb.Port))
	_, has =rack.DataNode[diskId]
	if has == false {
		rack.DataNode[diskId] = NewDataNode(diskId)
	}
	node = rack.DataNode[diskId]
	node.lastHeartbeat = model.Now().Unix()

	maxID := util.VIDType(0)
	for _, v := range hb.Volumes {
		if vi, err := volume.NewVolumeInfo(v); err == nil {
			if vi.Id > maxID {
				maxID = vi.Id
			}
			node.VolumeInfos[vi.Id] = &vi
		} else {
			glog.V(0).Infof("Fail to convert joined volume information: %v", err)
		}
	}
	if maxID > s.IDC.MaxVolumeId {
		s.IDC.MaxVolumeId = maxID
		s.RNode.Propose(MaxVolumeID, strconv.Itoa(int(maxID)))
	}

	// update max volume count
	freeChange := false
	if count := int(hb.MaxVolumeCount) - node.MaxVolumeCount;count != 0 {
		node.MaxVolumeCount += count
		rack.MaxVolumeCount += count
		center.MaxVolumeCount += count
		s.IDC.MaxVolumeCount += count
		freeChange = true
	}

	if count := int(len(hb.Volumes)) - node.volumeCount;count != 0 {
		node.volumeCount += count
		rack.volumeCount += count
		center.volumeCount += count
		s.IDC.volumeCount += count
		freeChange = true
	}

	if freeChange == true {
		node.FreeVolumeCount = node.MaxVolumeCount - node.volumeCount
		rack.FreeVolumeCount = rack.MaxVolumeCount - rack.volumeCount
		center.FreeVolumeCount = center.MaxVolumeCount - center.volumeCount
		s.IDC.FreeVolumeCount = s.IDC.MaxVolumeCount - s.IDC.volumeCount
	}
	node.Ip = hb.Ip
	node.Port = int(hb.Port)
	node.PublicUrl = hb.PublicUrl

	return true
}

// SendHeartbeat 处理心跳入口
func (s *ManageServer)SendHeartbeat(stream pb.Seaweed_SendHeartbeatServer) error  {
	if key, has:= s.RNode.Lookup(MaxVolumeID);has {
		maxVolumeID, _ := strconv.Atoi(key)
		s.IDC.MaxVolumeId = util.VIDType(maxVolumeID)
	}

	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			return err
		}
		glog.Info("%v", heartbeat)
		// 如果非Leader,那么让节点修改Leader
		if !s.RNode.ReadStatus().IsLeader {
			msg := &pb.HeartbeatResponse{
				Leader: s.RNode.ReadStatus().Leader}
			if err := stream.Send(msg); err != nil {
				glog.Error("Send failed! error info ：%s", err)
				return err
			}
		}
		// 更新存储信息
		if s.updateStore(heartbeat) == false {
			glog.Error(" update Store failed! error info ：%s", err)
		}
	}
}

// ClusterStatusHandler cluster status
func (s *ManageServer) ClusterStatusHandler(w http.ResponseWriter, r *http.Request) {
	stat := s.RNode.ReadStatus()

	bytes, err := json.Marshal(&stat)
	if err != nil {
		bytes = []byte("json marshal error")
	}
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(bytes)

//	s.RNode.Propose("1", "test")
	return
}

// DirStatusHandler dir status
func (s *ManageServer) DirStatusHandler(w http.ResponseWriter, r *http.Request) {
	bytes, err := json.Marshal(&s.IDC)
	if err != nil {
		bytes = []byte("json marshal error")
	}
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, err = w.Write(bytes)

	return
}

func (s *ManageServer) getVolumeGrowOption(r *http.Request) (*volume.GrowOption, error) {
	replicationString := r.FormValue("replication")
	if replicationString == "" {
		replicationString = *s.DefaultReplicaPlacement
	}
	replicaPlacement, err :=  replicate.NewPlacementFromString(replicationString)

	if err != nil {
		return nil, err
	}
	ttl, err := ttl.ReadTTL(r.FormValue("ttl"))
	if err != nil {
		return nil, err
	}

	volumeGrowOption := &volume.GrowOption{
		Collection:       r.FormValue("collection"),
		ReplicaPlacement: replicaPlacement,
		Ttl:              ttl,
		DataCenter:       r.FormValue("dataCenter"),
		Rack:             r.FormValue("rack"),
		DataNode:         r.FormValue("dataNode"),
	}
	return volumeGrowOption, nil
}

func (s *ManageServer) getNodeForDiffCenter(count int) (servers []*DataNode, err error) {
	if count > 0 && len(s.IDC.DataCenters) < count {
		err = errors.New(fmt.Sprintf("Only has %d center, not enough for %d.", len(s.IDC.DataCenters), count))
		return
	}
	for _, dcVal := range s.IDC.DataCenters {
		if count <= 0 {
			break
		}
		if dcVal.FreeVolumeCount <= 0 {
			continue
		}
		for _, dcRack := range dcVal.DataRack {
			if dcRack.FreeVolumeCount <= 0 {
				continue
			}
			for _, dcNode := range dcRack.DataNode {
				if dcNode.FreeVolumeCount <= 0 {
					continue
				}
				servers = append(servers, dcNode)
				break
			}
			break
		}
		count--
	}

	return
}

func (s *ManageServer) getNodeForDiffRack(count int) (servers []*DataNode, err error) {
	sum := count
	for _, center := range s.IDC.DataCenters {
		if center.FreeVolumeCount <= 0 {
			continue
		}
		for _, dcRack := range center.DataRack {
			if sum <= 0 {
				break
			}
			if dcRack.FreeVolumeCount <= 0 {
				continue
			}
			for _, dcNode := range dcRack.DataNode {
				if dcNode.FreeVolumeCount <= 0 {
					continue
				}
				servers = append(servers, dcNode)
				break
			}
			sum--
		}
		if sum <= 0 {
			break
		}
	}
	if sum > 0 {
		err = errors.New(fmt.Sprintf("Only has %d center, not enough for %d.", len(s.IDC.DataCenters), count))
	}

	return
}

func (s *ManageServer) getNodeForSameRack(count int) (servers []*DataNode, err error) {
	sum := count
	for _, center := range s.IDC.DataCenters {
		if center.FreeVolumeCount <= 0 {
			continue
		}
		for _, dcRack := range center.DataRack {
			if sum <= 0 {
				break
			}
			if dcRack.FreeVolumeCount <= 0 {
				continue
			}
			for _, dcNode := range dcRack.DataNode {
				if dcNode.FreeVolumeCount <= 0 {
					continue
				}
				servers = append(servers, dcNode)
				break
			}
			sum--
		}
		if sum <= 0 {
			break
		}
	}
	if sum > 0 {
		err = errors.New(fmt.Sprintf("Only has %d center, not enough for %d.", len(s.IDC.DataCenters), count))
	}

	return
}

func (s *ManageServer) findFreeDataNode(option *volume.GrowOption) (nds []*DataNode, err error) {
	rp := replicate.Placement{
		DiffDataCenterCount:option.ReplicaPlacement.DiffDataCenterCount,
		DiffRackCount:option.ReplicaPlacement.DiffRackCount,
		SameRackCount:option.ReplicaPlacement.SameRackCount}
	flag := true
	if rp.DiffDataCenterCount > 0 && flag == true {
		rp.DiffDataCenterCount += 1
		flag = false
	}
	if rp.DiffRackCount > 0  && flag == true {
		rp.DiffRackCount += 1
		flag = false
	}
	if rp.SameRackCount >= 0  && flag == true {
		rp.SameRackCount += 1
	}
	var tmpNds []*DataNode
	// 获取不同中心节点
	tmpNds, err = s.getNodeForDiffCenter(rp.DiffDataCenterCount)
	if  err != nil {
		return
	}
	for _, val := range tmpNds {
		nds = append(nds, val)
	}
	// 获取不同机架节点
	tmpNds, err = s.getNodeForDiffRack(rp.DiffRackCount)
	if err != nil {
		return
	}
	for _, val := range tmpNds {
		nds = append(nds, val)
	}
	// 获取相同机架节点
	tmpNds, err = s.getNodeForDiffRack(rp.SameRackCount)
	if err != nil {
		return
	}
	for _, val := range tmpNds {
		nds = append(nds, val)
	}

	return
}

func (s *ManageServer) AllocateVolume(dn *DataNode, vid util.VIDType, option *volume.GrowOption) error {
	values := make(url.Values)
	values.Add("volume", vid.String())
	values.Add("collection", option.Collection)
	values.Add("replication", option.ReplicaPlacement.String())
	values.Add("ttl", option.Ttl.String())
	jsonBlob, err := util.Post("http://"+ dn.Url() +"/admin/assign_volume", values)
	if err != nil {
		return err
	}
	type AllocateVolumeResult struct {
		Error string
	}
	var ret AllocateVolumeResult
	if err := json.Unmarshal(jsonBlob, &ret); err != nil {
		return fmt.Errorf("Invalid JSON result for %s: %s", "/admin/assign_volum", string(jsonBlob))
	}
	if ret.Error != "" {
		return errors.New(ret.Error)
	}

	return nil
}

func (s *ManageServer) GrowVolume(option *volume.GrowOption) (int, error) {
	s.IDC.MaxVolumeId += 1
	s.RNode.Propose(MaxVolumeID, strconv.Itoa(int(s.IDC.MaxVolumeId)))
	servers, err := s.findFreeDataNode(option)
	if err != nil {
		return 0, err
	}
	addLen := 0
	for _, server := range servers {
		vi := volume.VolumeInfo{
			Id:               s.IDC.MaxVolumeId,
			Size:             0,
			Collection:       option.Collection,
			ReplicaPlacement: option.ReplicaPlacement,
			Ttl:              option.Ttl,
			Version:          version.CurrentVersion,
		}
		if err := s.AllocateVolume(server, s.IDC.MaxVolumeId, option); err == nil {
			server.AddVolume(&vi)
			addLen++
		} else {
			glog.V(0).Infoln("Failed to assign volume",  s.IDC.MaxVolumeId, "to", servers, "error", err)
			return  addLen, fmt.Errorf("Failed to assign %d: %v",  s.IDC.MaxVolumeId, err)
		}
	}

	return addLen, nil
}

func (s *ManageServer) VolGrowHandler(w http.ResponseWriter, r *http.Request) {
	option, err := s.getVolumeGrowOption(r)
	if err != nil {
		util.WriteJsonError(w, r, http.StatusNotAcceptable, err)
		return
	}
	var count,counter,c int
	count, err = strconv.Atoi(r.FormValue("count"))
	if err != nil {
		util.WriteJsonError(w, r, http.StatusNotAcceptable, errors.New("parameter count is not found"))
		return
	}

	if s.IDC.FreeVolumeCount < count * option.ReplicaPlacement.GetCopyCount() {
		err = errors.New("Only " + strconv.Itoa(s.IDC.FreeVolumeCount) + " volumes left! Not enough for " +
			strconv.Itoa(count*option.ReplicaPlacement.GetCopyCount()))
		util.WriteJsonError(w, r, http.StatusNotAcceptable, err)
		return
	}

	for i := 0; i < count; i++ {
		if c, err = s.GrowVolume(option); err == nil {
			counter += c
		} else {
			glog.Error("Failed to Grow Volume %s", err.Error())
		}
	}

	util.WriteJsonQuiet(w, r, http.StatusOK, map[string]interface{}{"count": counter})

	return
}
