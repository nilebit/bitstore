package manageserver

import (
	"github.com/golang/glog"
	"github.com/gorilla/mux"
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

func (s *ManageServer) checkPeers() (address string, cleanedPeers []string)  {
	address = *s.Ip + ":" + strconv.Itoa(*s.Port)
	if *s.Peers != "" {
		cleanedPeers = strings.Split(*s.Peers, ",")
	}
	hasSelf := false
	for _, peer := range cleanedPeers {
		if peer == address {
			hasSelf = true
			break
		}
	}

	peerCount := len(cleanedPeers)
	if !hasSelf {
		peerCount += 1
	}
	if peerCount % 2 == 0 {
		glog.Fatalf("Only odd number of masters are supported!")
	}
	return
}

func (s *ManageServer) StartServer() bool {
//	address, peers := s.checkPeers()



	return true
}