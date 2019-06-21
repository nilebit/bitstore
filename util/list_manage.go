package util

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"time"

	"github.com/golang/glog"
)

type ClusterStatusResult struct {
	IsLeader bool     `json:"IsLeader,omitempty"`
	Leader   string   `json:"Leader,omitempty"`
	Peers    []string `json:"Peers,omitempty"`
}

func GrpcDial(ctx context.Context, address string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	// opts = append(opts, grpc.WithBlock())
	// opts = append(opts, grpc.WithTimeout(time.Duration(5*time.Second)))
	var options []grpc.DialOption
	options = append(options,
		// grpc.WithInsecure(),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    30 * time.Second, // client ping server if no activity for this long
			Timeout: 20 * time.Second,
		}))
	for _, opt := range opts {
		if opt != nil {
			options = append(options, opt)
		}
	}
	return grpc.DialContext(ctx, address, options...)
}

func ListManage(server string) (leader string, peers []string, err error) {
	jsonBlob, err := Get("http://" + server + "/cluster/status")
	glog.V(2).Info("list Manage result :", string(jsonBlob))
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
