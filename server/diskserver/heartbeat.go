package diskserver

import (
	"context"
	"errors"
	"fmt"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/pb"
	"github.com/nilebit/bitstore/util"
	"google.golang.org/grpc"
	"time"
)

func (s *DiskServer) ResetLeader() {
	if s.CurrentLeader != "" {
		s.CurrentLeader = ""
		glog.V(0).Infof("Resetting manage node leader.\n")
	}
}

func (s *DiskServer) FindLeader() (leader string, err error) {
	if len(s.ManageNode) == 0 {
		return "", errors.New("No manage node found!")
	}

	if s.CurrentLeader != "" {
		return s.CurrentLeader, nil
	}

	for _, m := range s.ManageNode {
		glog.V(4).Infof("Listing masters on %s", m)
		if leader, peers, e := util.ListManage(m); e == nil {
			if leader != "" {
				s.ManageNode = append(peers, m)
				s.CurrentLeader = leader
				glog.V(2).Infof("current manages nodes is %v", s.ManageNode)
				break
			}
		} else {
			glog.V(4).Infof("Failed listing manages on %s: %v", m, e)
		}
	}
	if s.CurrentLeader == "" {
		return "", errors.New("No manages node available!")
	}
	return s.CurrentLeader, nil
}

func (s *DiskServer) ResetAndFindLeader() (leader string, err error) {
	s.ResetLeader()
	return s.FindLeader()
}

func (s *DiskServer) CollectHeartbeat() *pb.Heartbeat {
	var volumeMessages []*pb.VolumeInformationMessage
	maxVolumeCount := 0
	var maxFileKey uint64
	for _, location := range s.Disk.Locations {
		maxVolumeCount += location.MaxVolumeCount
		location.Lock()
		for _, v := range location.Volumes {
			if maxFileKey < v.NM.MaxFileKey() {
				maxFileKey = v.NM.MaxFileKey()
			}
			if !v.Expired(s.Disk.VolumeSizeLimit) {
				volumeMessages = append(volumeMessages, v.ToVolumeInformationMessage())
			} else {
				if v.ExiredLongEnough(MAX_TTL_VOLUME_REMOVAL_DELAY) {
					location.DeleteVolumeById(v.Id)
					glog.V(0).Infoln("volume", v.Id, "is deleted.")
				} else {
					glog.V(0).Infoln("volume", v.Id, "is expired.")
				}
			}
		}
		location.Unlock()
	}

	return &pb.Heartbeat{
		Ip:             *s.Ip,
		Port:           uint32(*s.Port),
		MaxVolumeCount: uint32(maxVolumeCount),
		MaxFileKey:     maxFileKey,
		DataCenter:     *s.DataCenter,
		Rack:           *s.Rack,
		Volumes:        volumeMessages,
	}

}

func (s *DiskServer) DoHeartbeat(ctx context.Context, leader string, sleepInterval time.Duration) (newLeader string, err error) {
	grpcConection, err := util.GrpcDial(ctx, leader, grpc.WithInsecure())
	if err != nil {
		return "", fmt.Errorf("fail to dial %s : %v", leader, err)
	}
	defer grpcConection.Close()

	client := pb.NewSeaweedClient(grpcConection)
	stream, err := client.SendHeartbeat(ctx)
	if err != nil {
		glog.V(0).Infof("SendHeartbeat to %s: %v", leader, err)
		return "", err
	}
	glog.V(0).Infof("Heartbeat to %s\n", leader)
	doneChan := make(chan error, 1)

	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				doneChan <- err
				return
			}
			if in.GetVolumeSizeLimit() != 0 {
				s.Disk.VolumeSizeLimit = in.GetVolumeSizeLimit()
			}

			if in.GetLeader() != "" && leader != in.GetLeader() {
				glog.V(0).Infof("Disk server found a new master leader: %s instead of %s \n", in.GetLeader(), leader)
				newLeader = in.GetLeader()
				doneChan <- nil
				return
			}
		}
	}()

	if err = stream.Send(s.CollectHeartbeat()); err != nil {
		glog.V(0).Infof("Disk Server Failed to send heart beat to leader(%s): %s", leader, err.Error())
		return "", err
	}
	tickChan := time.Tick(sleepInterval)
	for {
		select {
		case <-tickChan:
			glog.V(4).Infof("Disk server %s:%d heartbeat\n", *(s.Ip), *(s.Port))
			if err = stream.Send(s.CollectHeartbeat()); err != nil {
				glog.V(0).Infof("Disk Server Failed to send heart beat to leader(%s): %s", leader, err.Error())
				return "", err
			}
		case err = <-doneChan:
			glog.V(0).Infof("Disk server heart beat stops with %v\n", err)
			return
		}
	}
}

func (s *DiskServer) Heartbeat() {
	glog.V(0).Infof("Disk server bootstraps with Manage server.")
	var err error
	var newLeader string

	for {
		if newLeader == "" {
			newLeader, err = s.ResetAndFindLeader()
			if err != nil {
				glog.Errorf("No Leader found: %s", err.Error())
				time.Sleep(time.Duration(*s.PulseSeconds) * time.Second)
				continue
			}
		}
		glog.V(0).Infof("Disk server communicates with manage server(%s) by heartbeat \n", newLeader)

		newLeader, err = s.DoHeartbeat(context.Background(), newLeader, time.Duration(*s.PulseSeconds)*time.Second)
		if err != nil {
			glog.V(0).Infof("heartbeat error: %s", err.Error())
			time.Sleep(time.Duration(*s.PulseSeconds) * time.Second)
			newLeader = ""
		}
	}
}
