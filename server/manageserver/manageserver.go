package manageserver

import (
	"github.com/golang/glog"
	"github.com/gorilla/mux"
	"github.com/nilebit/bitstore/raftnode"
	"github.com/nilebit/bitstore/raftnode/topology"
	"go.etcd.io/etcd/raft/raftpb"
	"net/http"
	"strconv"
	"strings"
)

type ManageServer struct {
	ID						*int
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
	address := "http://" + *s.Ip + ":" + strconv.Itoa(*s.Port + 100)
	peerCount := 0
	hasSelf := false
	if *s.Peers != "" {
		tempPeers := strings.Split(*s.Peers, ",")
		for _, peer := range tempPeers {
			ipPort := strings.Split(peer, ":")
			port, _ := strconv.Atoi(ipPort[1])
			newAddress := "http://"+ ipPort[0] + ":" + strconv.Itoa(port+100)
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

	// raft server

	go func() {
		peers := s.checkPeers()
		proposeC := make(chan string)
		defer close(proposeC)
		confChangeC := make(chan raftpb.ConfChange)
		defer close(confChangeC)
		// raft provides a commit stream for the proposals from the http api
		getSnapshot := func() ([]byte, error) { return s.topos.GetSnapshot() }

		commitC, errorC, SnapshotterReady := raftnode.NewRaftNode(*s.ID, peers, false, *s.MetaFolder, getSnapshot, proposeC, confChangeC)
		var topo *topology.Topology
		topo = topology.NewTopology(uint64(*s.VolumeSizeLimitMB), <-SnapshotterReady, proposeC, commitC, errorC)
		// the key-value http handler will propose updates to raft
		raftnode.ServeHttpKVAPI(topo, *s.Port + 1000, confChangeC, errorC)
	}()

	listeningAddress := *s.Ip + ":" + strconv.Itoa(*s.Port)
	glog.V(0).Infoln("Start a disk server ", "at", listeningAddress)
	// go s.heartbeat()

	if err := http.ListenAndServe(listeningAddress, s.Router); err != nil {
		glog.Fatalf("service fail to serve: %v", err)
		return false
	}

	return true
}