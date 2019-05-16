package manageserver

import (
	"github.com/nilebit/bitstore/pb/manage_server_pb"
	"github.com/golang/glog"
)

func (ms *MasterServer) SendHeartbeat(stream manager_server_pb.Bitstore_SendHeartbeatServer) error {
	var dn *topology.DataNode


	defer func() {
		if dn != nil {
			glog.V(0).Infof("unregister disconnected disk server %s:%d\n", dn.Ip, dn.Port)
		}
	}

	for {
		heartbeat, err := stream.Recv()
		if err != nil {
			return err
		}

		if dn == nil{

		}

		glog.V(4).Infof("master received hearbeat %s \n", heartbeat.String())

		newLeader, err := t.Leader()
		if err == nil {
			if err := stream.Send(&manage_server_pb.HeartbeatResponse{
				Leader: newLeader,
			}); err != nil {
				return err
			}
		}

	}
}
