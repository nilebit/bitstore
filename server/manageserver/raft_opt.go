package manageserver

import (
	"context"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/util"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"net/url"
	"os"
	"strconv"
)

// FileExist checking file
func FileExist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}


// Process Raft node process
func (rc *RaftNode) Process(ctx context.Context, m raftpb.Message) error { return rc.node.Step(ctx, m) }

// IsIDRemoved removed
func (rc *RaftNode) IsIDRemoved(id uint64) bool {return false }

// ReportUnreachable Report Unreachable
func (rc *RaftNode) ReportUnreachable(id uint64) {}

// ReportSnapshot Report Snapshot
func (rc *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

// ReadCommitC Read commitC
func (rc *RaftNode) ReadCommitC() <-chan *string { return rc.commitC }

// ReadErrorC Read ErrorC
func (rc *RaftNode) ReadErrorC() <-chan error { return rc.errorC }

// WriteProposeC Write Propose
func (rc *RaftNode) WriteProposeC() chan<- string { return rc.proposeC }

// WriteProposeC Write Propose
func (rc *RaftNode) WriteSnapshotter() chan<- *snap.Snapshotter { return rc.snapshotterReady }

// ReadStatus Read node status
func (rc *RaftNode) ReadStatus() (stat util.ClusterStatusResult) {
	nodestatus := rc.node.Status()

	if nodestatus.Lead == uint64(rc.id) {
		stat.IsLeader = true
	}
	temp, err := url.Parse(rc.members[nodestatus.Lead].Urls)
	if err == nil {
		post, _ := strconv.Atoi(temp.Port())
		stat.Leader = temp.Hostname() + ":" + strconv.Itoa(post-100)
	} else {
		glog.Fatalf("raftnode: Failed parsing URL (%v)", err)
	}
	for _, mem := range rc.members {
		temp, _ := url.Parse(mem.Urls)
		post, _ := strconv.Atoi(temp.Port())
		newHost := temp.Hostname() + ":" + strconv.Itoa(post-100)
		stat.Peers = append(stat.Peers, newHost)
	}

	return stat
}