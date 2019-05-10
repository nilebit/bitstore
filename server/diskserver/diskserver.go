package diskserver

import (
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/diskopt"
	"net/http"
	"strconv"
)

type DiskNodeMapper interface {
	RegistRouter()
	CreateDiskOpt()
	StartServer()
}

type DiskServer struct {
	Port                  *int
	Ip                    *string
	Cluster               *string
	MaxCpu                *int
	DataCenter            *string
	Rack                  *string
	Folders               []string
	FolderMaxLimits       []int
	Debug                 *bool
	Router          	  *mux.Router
	Disks			  	  *diskopt.Disk
	DiskNodeMapper
}

func NewDiskServer() *DiskServer {
	return &DiskServer{}
}

func (s *DiskServer)RegistRouter() {
	paramMux := mux.NewRouter().SkipClean(false)
	apiRouter := paramMux.NewRoute().PathPrefix("/").Subrouter()
	apiRouter.Methods("GET").Path("/status").HandlerFunc(s.Status)
	s.Router = apiRouter

	return
}

func (s *DiskServer)CreateDiskOpt() {
	s.Disks = diskopt.NewDisk(s.Folders, s.FolderMaxLimits)
	return
}

func (s *DiskServer) StartServer() {
	listeningAddress := *s.Ip + ":" + strconv.Itoa(*s.Port)
	glog.V(0).Infoln("Start a disk server ", "at", listeningAddress)

	if err := http.ListenAndServe(listeningAddress, s.Router); err != nil {
		glog.Fatalf("service fail to serve: %v", err)
	}
}