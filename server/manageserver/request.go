package manageserver

import (
	"encoding/json"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/pb"
	"github.com/nilebit/bitstore/raftnode/topology"
	"net/http"
	"runtime"
	"runtime/debug"
)
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

func (s *ManageServer) SendHeartbeat(stream pb.Seaweed_SendHeartbeatServer) error {
	var dn *topology.DataNode
	for {
		_, err := stream.Recv()
		if err != nil {
			if dn != nil {
				glog.V(0).Infof("lost disk node server %s:%d", dn.Ip, dn.Port)
				//		t.UnRegisterDataNode(dn)
			}
			return err
		}

		if dn == nil {

		}
	}
}