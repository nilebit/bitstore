package manageserver

import (
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/raftnode"
	"github.com/nilebit/bitstore/raftnode/topology"
	"go.etcd.io/etcd/raft/raftpb"
	"strconv"
	"strings"
)

type ManageServer struct {
	Ip 						*string
	Port                    *int
	MetaFolder              *string
	VolumeSizeLimitMB       *uint
	Peers					*string
	MaxCpu					*int
	Router          		*mux.Router
	topos 					*topology.Topology
}

func NewManageServer() *ManageServer {
	return &ManageServer{}
}

func (s *ManageServer) RegistRouter() {
	paramMux := mux.NewRouter().SkipClean(false)
	apiRouter := paramMux.NewRoute().PathPrefix("/").Subrouter()
	apiRouter.Methods("GET").Path("/status").HandlerFunc(s.StatusHandler)

	s.Router = apiRouter
}

func (s *ManageServer) checkPeers() (cleanedPeers []string)  {
	address := *s.Ip + ":" + strconv.Itoa(*s.Port + 10000)
	peerCount := 0
	hasSelf := false
	if *s.Peers != "" {
		tempPeers := strings.Split(*s.Peers, ",")
		for _, peer := range tempPeers {
			ipPort := strings.Split(peer, ":")
			port, _ := strconv.Atoi(ipPort[1])
			newAddress := ipPort[0] + ":" + strconv.Itoa(port+10000)
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

	if peerCount % 2 == 0 {
		glog.Fatalf("Only odd number of manage are supported!")
	}
	return
}

func (s *ManageServer) StartServer() bool {
	peers := s.checkPeers()
	// raft server
	go func() {
		proposeC := make(chan string)
		defer close(proposeC)
		confChangeC := make(chan raftpb.ConfChange)
		defer close(confChangeC)
		// raft provides a commit stream for the proposals from the http api
		getSnapshot := func() ([]byte, error) { return s.topos.GetSnapshot() }

		commitC, errorC, SnapshotterReady := raftnode.NewRaftNode(0x01, peers, false, *s.MetaFolder, getSnapshot, proposeC, confChangeC)
		topology.NewTopology(uint64(*s.VolumeSizeLimitMB), <-SnapshotterReady, proposeC, commitC, errorC)
	}()


	return true
}