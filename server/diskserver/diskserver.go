package diskserver

import (
	"errors"
	"net/http"
	"strconv"
	"sync"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/diskopt"

	"github.com/nilebit/bitstore/pb"
	"github.com/nilebit/bitstore/util"
)

const (
	MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes
)

type DiskServer struct {
	Port            *int
	Ip              *string
	Cluster         *string
	MaxCpu          *int
	DataCenter      *string
	Rack            *string
	Folders         []string
	FolderMaxLimits []int
	Debug           *bool
	Router          *mux.Router
	Disk            *diskopt.Disk
	CurrentLeader   string
	ManageNode      []string
	DiskServerLock  sync.RWMutex
	PulseSeconds    int
}

func NewDiskServer() *DiskServer {
	return &DiskServer{}
}

func (s *DiskServer) RegistRouter() {
	paramMux := mux.NewRouter().SkipClean(false)
	apiRouter := paramMux.NewRoute().PathPrefix("/").Subrouter()
	apiRouter.Methods("POST").Path("/admin/assign_volume").HandlerFunc(s.AssignVolumeHandler)

	apiRouter.Methods("GET").Path("/status").HandlerFunc(s.StatusHandler)
	apiRouter.Methods("PUT", "POST").Path("/{object:.+}").HandlerFunc(s.PostHandler)
	apiRouter.Methods("GET", "HEAD").Path("/{object:.+}").HandlerFunc(s.GetHandler)
	apiRouter.Methods("DELETE").Path("/{object:.+}").HandlerFunc(s.DeleteHandler)

	s.Router = apiRouter
}

func (s *DiskServer) CreateDiskOpt() {
	s.Disk = diskopt.NewDisk(s.Folders, s.FolderMaxLimits, *s.Ip, *s.Port)
	return
}

func (s *DiskServer) StartServer() bool {
	listeningAddress := *s.Ip + ":" + strconv.Itoa(*s.Port)
	glog.V(0).Infoln("Start a disk server ", "at", listeningAddress)
	// go s.heartbeat()

	if err := http.ListenAndServe(listeningAddress, s.Router); err != nil {
		glog.Fatalf("service fail to serve: %v", err)
		return false
	}

	return true
}

func (s *DiskServer) GetMaster() string {
	s.DiskServerLock.RLock()
	defer s.DiskServerLock.RUnlock()
	return s.CurrentLeader
}

func (s *DiskServer) SetMaster(masterNode string) {
	s.DiskServerLock.RLock()
	defer s.DiskServerLock.RUnlock()
	s.CurrentLeader = masterNode
}

func (s *DiskServer) FindMaster() (leader string, err error) {
	if len(s.ManageNode) == 0 {
		return "", errors.New("No master node found!")
	}

	if s.CurrentLeader != "" {
		return s.CurrentLeader, nil
	}
	for _, m := range s.ManageNode {
		glog.V(4).Infof("Listing masters on %s", m)
		if leader, masters, e := util.ListMasters(m); e == nil {
			if leader != "" {
				s.ManageNode = append(masters, m)
				s.CurrentLeader = leader
				glog.V(2).Infof("current master nodes is %v", s.ManageNode)
				break
			}
		} else {
			glog.V(4).Infof("Failed listing masters on %s: %v", m, e)
		}
	}
	if s.CurrentLeader == "" {
		return "", errors.New("No master node available!")
	}
	return s.CurrentLeader, nil
}

func (s *DiskServer) ResetAndFindMaster() (leader string, err error) {
	s.ResetMaster()
	return s.FindMaster()
}

func (s *DiskServer) ResetMaster() {
	if s.CurrentLeader != "" {
		s.CurrentLeader = ""
		glog.V(0).Infof("Resetting master node\n")
	}
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
