package manageserver

import (
	"net/http"
	"net/url"
	"strconv"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/pb"
	"github.com/nilebit/bitstore/util"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// ManageServer 管理节点结构
type ManageServer struct {
	MetaFolder        *string
	VolumeSizeLimitMB *uint
	Cluster           *string
	Advertise         *string
	MaxCPU            *int
	Router            *mux.Router
	RNode             *RaftNode
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
	var err error
	s.RNode, err = newRaftNode(*s.Advertise, *s.Cluster, false, *s.MetaFolder)
	if err != nil {
		glog.Fatalf("manage node server failed to new raft node: %v", err)
	}
	// start a manage node server
	host, _ := url.Parse(*s.Advertise)
	post, _ := strconv.Atoi(host.Port())
	listeningAddress := host.Hostname() + ":" + strconv.Itoa(post-100)
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
