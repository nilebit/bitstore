package manageserver

import (
	"encoding/json"
	"errors"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/replicate"
	"github.com/nilebit/bitstore/diskopt/ttl"
	"github.com/nilebit/bitstore/diskopt/volume"
	"github.com/nilebit/bitstore/util"
	"github.com/prometheus/common/model"
	"net/http"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/nilebit/bitstore/pb"
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
	s.RNode.mu.Lock()
	defer s.RNode.mu.Unlock()
	if hb.DataCenter == "" {
		hb.DataCenter = DefaultCenterName
	}

	_, has := s.RNode.store.DataCenters[hb.DataCenter]
	if has == false {
		s.RNode.store.DataCenters[hb.DataCenter] = NewDataCenter()
	}

	if hb.Rack == "" {
		hb.Rack = DefaultRackName
	}
	_, has = s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack]
	if has == false {
		s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack] = NewRack()
	}

	diskId := hb.Ip + ":" + strconv.Itoa(int(hb.Port))
	_, has = s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId]
	if has == false {
		s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId] = NewDataNode(diskId)
	}
	s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].lastHeartbeat = model.Now().Unix()

	for _, v := range hb.Volumes {
		if vi, err := volume.NewVolumeInfo(v); err == nil {
			s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].VolumeInfos[vi.Id] = &vi
		} else {
			glog.V(0).Infof("Fail to convert joined volume information: %v", err)
		}
	}
	// update max volume count
	freeChange := false
	if count := int(hb.MaxVolumeCount) - s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].MaxVolumeCount;count != 0 {
		s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].MaxVolumeCount += count
		s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].MaxVolumeCount += count
		s.RNode.store.DataCenters[hb.DataCenter].MaxVolumeCount += count
		s.RNode.store.MaxVolumeCount += count
		freeChange = true
	}

	if count := int(len(hb.Volumes)) - s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].volumeCount;count != 0 {
		s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].volumeCount += count
		s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].volumeCount += count
		s.RNode.store.DataCenters[hb.DataCenter].volumeCount += count
		s.RNode.store.volumeCount += count
		freeChange = true
	}

	if freeChange == true {
		s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].FreeVolumeCount =
			s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].MaxVolumeCount -
				s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].DataNode[diskId].volumeCount
		s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].FreeVolumeCount =
			s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].MaxVolumeCount -
				s.RNode.store.DataCenters[hb.DataCenter].DataRack[hb.Rack].volumeCount
		s.RNode.store.DataCenters[hb.DataCenter].FreeVolumeCount =
			s.RNode.store.DataCenters[hb.DataCenter].MaxVolumeCount -
				s.RNode.store.DataCenters[hb.DataCenter].volumeCount
		s.RNode.store.FreeVolumeCount = s.RNode.store.MaxVolumeCount - s.RNode.store.volumeCount
	}

	return true
}

// SendHeartbeat 处理心跳入口
func (s *ManageServer)SendHeartbeat(stream pb.Seaweed_SendHeartbeatServer) error  {
	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			return err
		}
		glog.Info("%v", heartbeat)
		// 更新存储信息
		if s.updateStore(heartbeat) == false {
			glog.Error(" update Store failed! error info ：%s", err)
		}

		// 如果非Leader,那么让节点修改Leader
		if !s.RNode.ReadStatus().IsLeader {
			msg := &pb.HeartbeatResponse{
				Leader: s.RNode.ReadStatus().Leader}
			if err := stream.Send(msg); err != nil {
				glog.Error("Send failed! error info ：%s", err)
				return err
			}
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
	bytes, err := json.Marshal(&s.RNode.store)
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

func (s *ManageServer) VolGrowHandler(w http.ResponseWriter, r *http.Request) {
	option, err := s.getVolumeGrowOption(r)
	if err != nil {
		util.WriteJsonError(w, r, http.StatusNotAcceptable, err)
		return
	}
	var count int
	count, err = strconv.Atoi(r.FormValue("count"))
	if err == nil {
		if s.RNode.FreeSpace() < count * option.ReplicaPlacement.GetCopyCount() {
			err = errors.New("Only " + strconv.Itoa(s.RNode.FreeSpace()) + " volumes left! Not enough for " +
				strconv.Itoa(count*option.ReplicaPlacement.GetCopyCount()))
		} else {
			//  TODO
			// count, err = s.GrowByCountAndType(count, option)
		}
	} else {
		err = errors.New("parameter count is not found")
	}

	if err != nil {
		util.WriteJsonError(w, r, http.StatusNotAcceptable, err)
	} else {
		util.WriteJsonQuiet(w, r, http.StatusOK, map[string]interface{}{"count": count})
	}

	return
}
