package diskserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/pb"
	"github.com/nilebit/bitstore/util"
	"google.golang.org/grpc"
)

func (s *DiskServer) resetLeader() {
	if s.CurrentLeader != "" {
		s.CurrentLeader = ""
		glog.V(0).Infof("Resetting manage node leader.\n")
	}
}

func (s *DiskServer) findLeader() (leader string, err error) {
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

func (s *DiskServer) collectHeartbeat() *pb.Heartbeat {
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

func (s *DiskServer) doHeartbeat(ctx context.Context, sleepInterval time.Duration) (err error) {
	grpcConection, err := grpc.Dial(s.CurrentLeader, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("fail to dial: %v", err)
	}
	defer grpcConection.Close()

	client := pb.NewSeaweedClient(grpcConection)
	stream, err := client.SendHeartbeat(ctx)
	if err != nil {
		glog.V(0).Infof("%v.SendHeartbeat(_) = _, %v", client, err)
		return err
	}

	// 接收与处理心跳
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

			if in.GetLeader() != "" && s.CurrentLeader != in.GetLeader() {
				glog.V(0).Infof("Disk server found a new master leader: %s instead of %s \n", in.GetLeader(), s.CurrentLeader)
				s.CurrentLeader = in.GetLeader()
				doneChan <- nil
				return
			}
		}
	}()

	// 定时发送心跳
	if err = stream.Send(s.collectHeartbeat()); err != nil {
		glog.V(0).Infof("Disk Server Failed to send heart beat to leader(%s): %s", s.CurrentLeader, err.Error())
		return  err
	}
	tickChan := time.Tick(sleepInterval)
	for {
		select {
		case <-tickChan:
			glog.V(4).Infof("Disk server %s:%d heartbeat\n", *(s.Ip), *(s.Port))
			if err = stream.Send(s.collectHeartbeat()); err != nil {
				glog.V(0).Infof("Disk Server Failed to send heart beat to leader(%s): %s", s.CurrentLeader, err.Error())
				return err
			}
		case err = <-doneChan:
			glog.V(0).Infof("Disk server heart beat stops with %v\n", err)
			return
		}
	}
}

// Heartbeat 开启心跳
func (s *DiskServer) Heartbeat() {
	glog.V(0).Infof("Disk server bootstraps with Manage server.")
	var err error

	for {
		if s.CurrentLeader == "" {
			s.resetLeader()
			if s.CurrentLeader, err = s.findLeader(); err != nil {
				glog.Errorf("No Leader found: %s", err.Error())
				time.Sleep(time.Duration(*s.PulseSeconds) * time.Second)
			}
		}
		glog.V(0).Infof("Disk server communicates with manage server(%s) by heartbeat \n", s.CurrentLeader)

		err := s.doHeartbeat(context.Background(), time.Duration(*s.PulseSeconds)*time.Second)
		if err != nil {
			glog.V(0).Infof("heartbeat error: %s", err.Error())
			time.Sleep(time.Duration(*s.PulseSeconds) * time.Second)
		}
	}
}
