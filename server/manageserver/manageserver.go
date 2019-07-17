package manageserver

import (
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/pb"
	"github.com/nilebit/bitstore/topology"
	"github.com/nilebit/bitstore/util"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net/http"
	"strconv"
)

// ManageServer 管理节点结构
type ManageServer struct {
	IP                *string
	Port              *int
	MetaFolder        *string
	VolumeSizeLimitMB *uint
	Cluster           *string
	Advertise		  *string
	MaxCPU            *int
	Router            *mux.Router
	topos             *topology.Topology
}

func NewManageServer() *ManageServer {
	return &ManageServer{}
}

// RegistRouter 注册ROUTER
func (s *ManageServer) RegistRouter() {
	paramMux := mux.NewRouter().SkipClean(false)
	apiRouter := paramMux.NewRoute().PathPrefix("/").Subrouter()
	apiRouter.Methods("GET").Path("/status").HandlerFunc(s.StatusHandler)
	apiRouter.Methods("GET").Path("/cluster/status").HandlerFunc(s.ClusterStatusHandler)

	s.Router = apiRouter
}

// StartServer start a rpc and http service
func (s *ManageServer) StartServer() bool {
	// raft server
	go func() {
		getSnapshot := func() ([]byte, error) { return s.topos.GetSnapshot() }
		rc, err := topology.NewRaftNode(*s.Advertise, *s.Cluster, false, *s.MetaFolder, getSnapshot)
		if err != nil {
			glog.Fatalf("manage node server failed to new raft node: %v", err)
		}
		// new topology
		s.topos = topology.NewTopology(uint64(*s.VolumeSizeLimitMB), rc)
	}()
	// start a manage node server
	listeningAddress := *s.IP + ":" + strconv.Itoa(*s.Port)
	listener, e := util.NewListener(listeningAddress, 0)
	if e != nil {
		glog.Fatalf("manage node server startup error: %v", e)
	}
	m := cmux.New(listener)
	grpcL := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpL := m.Match(cmux.Any())

	// Create your protocol servers.
	grpcS := grpc.NewServer()
	pb.RegisterSeaweedServer(grpcS, s)
	reflection.Register(grpcS)

	httpS := &http.Server{Handler: s.Router}

	go grpcS.Serve(grpcL)
	go httpS.Serve(httpL)

	if err := m.Serve(); err != nil {
		glog.Fatalf("manage node server failed to serve: %v", err)
	}

	return true
}
