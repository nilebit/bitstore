package manageserver

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/pb"
	"github.com/nilebit/bitstore/topology"
	"github.com/nilebit/bitstore/util"
	"github.com/soheilhy/cmux"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type ManageServer struct {
	ID                *int
	Ip                *string
	Port              *int
	MetaFolder        *string
	VolumeSizeLimitMB *uint
	Peers             *string
	MaxCpu            *int
	Router            *mux.Router
	topos             *topology.Topology
}

func NewManageServer() *ManageServer {
	return &ManageServer{}
}

func (s *ManageServer) RegistRouter() {
	paramMux := mux.NewRouter().SkipClean(false)
	apiRouter := paramMux.NewRoute().PathPrefix("/").Subrouter()
	apiRouter.Methods("GET").Path("/status").HandlerFunc(s.StatusHandler)
	apiRouter.Methods("GET").Path("/cluster/status").HandlerFunc(s.ClusterStatusHandler)

	s.Router = apiRouter
}

func (s *ManageServer) checkPeers() (cleanedPeers []string) {
	address := "http://" + *s.Ip + ":" + strconv.Itoa(*s.Port+100)
	peerCount := 0
	hasSelf := false
	if *s.Peers != "" {
		tempPeers := strings.Split(*s.Peers, ",")
		for _, peer := range tempPeers {
			ipPort := strings.Split(peer, ":")
			port, _ := strconv.Atoi(ipPort[1])
			newAddress := "http://" + ipPort[0] + ":" + strconv.Itoa(port+100)
			if address == newAddress {
				hasSelf = true
			}
			cleanedPeers = append(cleanedPeers, newAddress)
			peerCount++
		}
	}

	if hasSelf == false {
		cleanedPeers = append(cleanedPeers, address)
		peerCount++
	}

	if peerCount%2 == 0 {
		glog.Fatalf("Only odd number of manage are supported!")
	}
	return
}

// StartServer start a rpc and http service
func (s *ManageServer) StartServer() bool {
	// raft server
	go func() {
		// raft provides a commit stream for the proposals from the http api
		getSnapshot := func() ([]byte, error) { return s.topos.GetSnapshot() }
		rc := topology.NewRaftNode(*s.ID, s.checkPeers(), false, *s.MetaFolder, getSnapshot)
		// new topology
		s.topos = topology.NewTopology(uint64(*s.VolumeSizeLimitMB), rc)
	}()
	// start a manage node server
	listeningAddress := *s.Ip + ":" + strconv.Itoa(*s.Port)
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
