package diskserver

import (
	"errors"
	"net/http"
	"strconv"
	"sync"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/diskopt"
	"github.com/nilebit/bitstore/operation"
	"github.com/nilebit/bitstore/pb/manage_server_pb"
)

type DiskNodeMapper interface {
	RegistRouter()
	CreateDiskOpt()
	StartServer() bool
}

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
	Disks           *diskopt.Disk
	CurrentLeader   string
	ManageNode      []string
	DiskNodeMapper
	DiskServerLock sync.RWMutex
	MasterNode     string
	PulseSeconds   int
}

func NewDiskServer() *DiskServer {
	return &DiskServer{}
}

func (s *DiskServer) RegistRouter() {
	paramMux := mux.NewRouter().SkipClean(false)
	apiRouter := paramMux.NewRoute().PathPrefix("/").Subrouter()
	apiRouter.Methods("GET").Path("/status").HandlerFunc(s.StatusHandler)
	apiRouter.Methods("PUT", "POST").Path("/{object:.+}").HandlerFunc(s.PostHandler)
	apiRouter.Methods("GET", "HEAD").Path("/{object:.+}").HandlerFunc(s.GetHandler)
	apiRouter.Methods("DELETE").Path("/{object:.+}").HandlerFunc(s.DeleteHandler)

	s.Router = apiRouter
}

func (s *DiskServer) CreateDiskOpt() {
	s.Disks = diskopt.NewDisk(s.Folders, s.FolderMaxLimits, *s.Ip, *s.Port)
	return
}

func (s *DiskServer) StartServer() bool {
	listeningAddress := *s.Ip + ":" + strconv.Itoa(*s.Port)
	glog.V(0).Infoln("Start a disk server ", "at", listeningAddress)
	go s.heartbeat()

	if err := http.ListenAndServe(listeningAddress, s.Router); err != nil {
		glog.Fatalf("service fail to serve: %v", err)
		return false
	}

	return true
}

func (s *DiskServer) GetMaster() string {
	s.DiskServerLock.RLock()
	defer s.DiskServerLock.RUnlock()
	return s.MasterNode
}

func (s *DiskServer) SetMaster(masterNode string) {
	s.DiskServerLock.RLock()
	defer s.DiskServerLock.RUnlock()
	s.MasterNode = masterNode
}

func (s *DiskServer) FindMaster() (leader string, err error) {
	if len(s.ManageNode) == 0 {
		return "", errors.New("No master node found!")
	}

	if s.MasterNode != "" {
		return s.MasterNode, nil
	}
	for _, m := range s.ManageNode {
		glog.V(4).Infof("Listing masters on %s", m)
		if leader, masters, e := operation.ListMasters(m); e == nil {
			if leader != "" {
				s.ManageNode = append(masters, m)
				s.MasterNode = leader
				glog.V(2).Infof("current master nodes is %v", s.ManageNode)
				break
			}
		} else {
			glog.V(4).Infof("Failed listing masters on %s: %v", m, e)
		}
	}
	if s.MasterNode == "" {
		return "", errors.New("No master node available!")
	}
	return s.MasterNode, nil
}

func (s *DiskServer) ResetAndFindMaster() (leader string, err error) {
	s.ResetMaster()
	return s.FindMaster()
}

func (s *DiskServer) ResetMaster() {
	if s.MasterNode != "" {
		s.MasterNode = ""
		glog.V(0).Infof("Resetting master node\n")
	}
}

func (s *DiskServer) CollectHeartbeat() *manage_server_pb.Heartbeat {
	return nil
}
