package diskserver

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/pb/manage_server_pb"
	"github.com/nilebit/bitstore/util"
	"google.golang.org/grpc"
)

func (s *DiskServer) heartbeat() {
	glog.V(0).Infof("Disk server start communicates with manage server(%s) by heartbeat \n", s.GetMaster())

	var err error
	var newLeader string
	grpcDialOption := grpc.WithInsecure()

	for {
		if newLeader == "" {
			newLeader, err = s.ResetAndFindMaster()
			if err != nil {
				glog.Errorf("No master found: %s", err.Error())
				continue
			}
		}

		glog.V(0).Infof("Disk server communicates with manage server(%s) by heartbeat \n", newLeader)
		newLeader, err = s.doHeartbeat(context.Background(), newLeader, grpcDialOption, time.Duration(s.PulseSeconds)*time.Second)
		if err != nil {
			glog.V(0).Infof("heartbeat error: %s", err.Error())
			time.Sleep(time.Duration(s.PulseSeconds) * time.Second)
			newLeader = ""
		}
	}
}

func (s *DiskServer) doHeartbeat(ctx context.Context, masterNode string, grpcDialOption grpc.DialOption, sleepInterval time.Duration) (newLeader string, err error) {
	grpcConection, err := util.GrpcDial(ctx, masterNode, grpcDialOption)
	if err != nil {
		return "", fmt.Errorf("fail to dial %s : %v", masterNode, err)
	}
	defer grpcConection.Close()

	client := manage_server_pb.NewBitstoreClient(grpcConection)
	stream, err := client.SendHeartbeat(ctx)
	if err != nil {
		glog.V(0).Infof("SendHeartbeat to %s: %v", masterNode, err)
		return "", err
	}

	s.SetMaster(masterNode)
	glog.V(0).Infof("Heartbeat to %s\n", masterNode)

	doneChan := make(chan error, 1)

	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				doneChan <- err
				return
			}
			if in.GetVolumeSizeLimit() != 0 {

			}

			if in.GetSecretKey() != "" {
			}
			if in.GetLeader() != "" && masterNode != in.GetLeader() {
				glog.V(0).Infof("Disk server found a new master leader: %s instead of %s \n", in.GetLeader(), masterNode)
				newLeader = in.GetLeader()
				doneChan <- nil
				return
			}
		}
	}()

	if err = stream.Send(s.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Disk Server Failed to send heart beat to master(%s): %s", masterNode, err.Error())
		return "", err
	}

	tickChan := time.Tick(sleepInterval)

	for {
		select {
		case <-tickChan:
			glog.V(4).Infof("Disk server %s:%d heartbeat\n", *(s.Ip), *(s.Port))
			if err = stream.Send(s.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Disk Server Failed to send heart beat to master(%s): %s", masterNode, err.Error())
				return "", err
			}
		case err = <-doneChan:
			glog.V(0).Infof("Disk server heart beat stops with %v\n", err)
			return
		}
	}
}
