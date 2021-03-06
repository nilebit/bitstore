package diskserver

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/diskopt"
)

const (
	MAX_TTL_VOLUME_REMOVAL_DELAY = 10 // 10 minutes
)

type DiskServer struct {
	Port            *int
	Ip              *string
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
	PulseSeconds    *int
}

func NewDiskServer() *DiskServer {
	return &DiskServer{}
}

func (s *DiskServer) RegistRouter() {
	paramMux := mux.NewRouter().SkipClean(false)
	apiRouter := paramMux.NewRoute().PathPrefix("/").Subrouter()
	apiRouter.Methods("POST").Path("/admin/assign_volume").HandlerFunc(s.AssignVolumeHandler)
	apiRouter.Methods("POST", "PUT", "GET", "DELETE").Path("/admin/volume/delete").HandlerFunc(s.VolumeDeleteHandler)
	apiRouter.Methods("POST").Path("/admin/vacuum/check").HandlerFunc(s.VacuumVolumeCheckHandler)
	apiRouter.Methods("POST").Path("/admin/vacuum/compact").HandlerFunc(s.VacuumVolumeCompactHandler)
	apiRouter.Methods("POST").Path("/admin/vacuum/commit").HandlerFunc(s.VacuumVolumeCommitHandler)

	apiRouter.Methods("GET").Path("/status").HandlerFunc(s.StatusHandler)
	apiRouter.Methods("PUT", "POST").Path("/{object:.+}").HandlerFunc(s.PostHandler)
	apiRouter.Methods("GET", "HEAD").Path("/{object:.+}").HandlerFunc(s.GetHandler)
	apiRouter.Methods("DELETE").Path("/{object:.+}").HandlerFunc(s.DeleteHandler)

	s.Router = apiRouter
}

// CreateDiskOpt 创建新磁盘
func (s *DiskServer) CreateDiskOpt() {
	s.Disk = diskopt.NewDisk(s.Folders, s.FolderMaxLimits, *s.Ip, *s.Port)
	// 保持与管理服务心跳
	go s.Heartbeat()
	return
}

// StartServer 启动服务
func (s *DiskServer) StartServer() bool {
	listeningAddress := *s.Ip + ":" + strconv.Itoa(*s.Port)
	glog.V(0).Infoln("Start a disk server ", "at", listeningAddress)

	if err := http.ListenAndServe(listeningAddress, s.Router); err != nil {
		glog.Fatalf("service fail to serve: %v", err)
		return false
	}

	return true
}
