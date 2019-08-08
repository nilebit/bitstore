package manageserver

import (
	"encoding/json"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/volume"
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

	_, has := s.RNode.store[hb.DataCenter]
	if has == false {
		s.RNode.store[hb.DataCenter] = NewDataCenter()
	}

	if hb.Rack == "" {
		hb.Rack = DefaultRackName
	}
	_, has = s.RNode.store[hb.DataCenter].dataRack[hb.Rack]
	if has == false {
		s.RNode.store[hb.DataCenter].dataRack[hb.Rack] = NewRack()
	}

	diskId := hb.Ip + ":" + strconv.Itoa(int(hb.Port))
	_, has = s.RNode.store[hb.DataCenter].dataRack[hb.Rack].dataNode[diskId]
	if has == false {
		s.RNode.store[hb.DataCenter].dataRack[hb.Rack].dataNode[diskId] = NewDataNode(diskId)
	}
	s.RNode.store[hb.DataCenter].dataRack[hb.Rack].dataNode[diskId].lastHeartbeat = model.Now().Unix()

	for _, v := range hb.Volumes {
		if vi, err := volume.NewVolumeInfo(v); err == nil {
			s.RNode.store[hb.DataCenter].dataRack[hb.Rack].dataNode[diskId].volumeInfos[vi.Id] = &vi
		} else {
			glog.V(0).Infof("Fail to convert joined volume information: %v", err)
		}
	}
	// update max volume count
	if count := int(hb.MaxVolumeCount) - s.RNode.store[hb.DataCenter].dataRack[hb.Rack].dataNode[diskId].maxVolumeCount;count != 0 {
		s.RNode.store[hb.DataCenter].dataRack[hb.Rack].dataNode[diskId].maxVolumeCount += count
		s.RNode.store[hb.DataCenter].dataRack[hb.Rack].maxVolumeCount += count
		s.RNode.store[hb.DataCenter].maxVolumeCount += count
	}

	if count := int(len(hb.Volumes)) - s.RNode.store[hb.DataCenter].dataRack[hb.Rack].dataNode[diskId].volumeCount;count != 0 {
		s.RNode.store[hb.DataCenter].dataRack[hb.Rack].dataNode[diskId].volumeCount += count
		s.RNode.store[hb.DataCenter].dataRack[hb.Rack].volumeCount += count
		s.RNode.store[hb.DataCenter].volumeCount += count
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
