package util

import (
	"encoding/json"

	"github.com/golang/glog"
)

type ClusterStatusResult struct {
	IsLeader bool     `json:"IsLeader,omitempty"`
	Leader   string   `json:"Leader,omitempty"`
	Peers    []string `json:"Peers,omitempty"`
}

func ListMasters(server string) (leader string, peers []string, err error) {
	jsonBlob, err := Get("http://" + server + "/cluster/status")
	glog.V(2).Info("list masters result :", string(jsonBlob))
	if err != nil {
		return "", nil, err
	}
	var ret ClusterStatusResult
	err = json.Unmarshal(jsonBlob, &ret)
	if err != nil {
		return "", nil, err
	}
	peers = ret.Peers
	if ret.IsLeader {
		peers = append(peers, ret.Leader)
	}
	return ret.Leader, peers, nil
}
