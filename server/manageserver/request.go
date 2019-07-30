package manageserver

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/diskopt/volume"
	"net/http"
	"runtime"
	"runtime/debug"

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

// SendHeartbeat deal Heartbeat
func (s *ManageServer)SendHeartbeat(stream pb.Seaweed_SendHeartbeatServer) error  {
	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			return err
		}
		var volumeInfos []volume.VolumeInfo
		for _, v := range heartbeat.Volumes {
			if vi, err := volume.NewVolumeInfo(v); err == nil {
				volumeInfos = append(volumeInfos, vi)
			} else {
				glog.V(0).Infof("Fail to convert joined volume information: %v", err)
			}
		}

		hb, _ := json.Marshal(heartbeat)
		fmt.Println(hb)
		if err := stream.Send(&pb.HeartbeatResponse{
			Leader: s.RNode.ReadStatus().Leader,
		}); err != nil {
			return err
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

	s.RNode.Propose("1", "test")
	return
}
