package manageserver

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"github.com/golang/glog"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.uber.org/zap"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
)
const (
	ElectionTickTime        int    = 10      // ElectionTickTime 选举超时
	HeartbeatTickTime       int    = 5       // HeartbeatTickTime 心跳超时
	MaxSizePerMsg           uint64 = 4194304 // MaxSizePerMsg 每包最大值
	defaultSnapshotCount    uint64 = 2
	snapshotCatchUpEntriesN uint64 = 1
)

type Member struct {
	ID   uint64
	Urls string
}

// RaftNode A key-value stream backed by raft
type RaftNode struct {
	proposeC         chan string            // proposed messages
	confChangeC      chan raftpb.ConfChange // proposed cluster config changes
	commitC          chan *string           // entries committed to glog
	errorC           chan error             // errors from raft session
	id               uint64                 // client ID for raft session
	members          map[uint64]*Member     // raft peer URLs
	join             bool                   // node is joining an existing cluster
	waldir           string                 // path to WAL directory
	snapdir          string                 // path to snapshot directory
	lastIndex        uint64                 // index of log at start
	confState        raftpb.ConfState
	snapshotIndex    uint64
	appliedIndex     uint64
	node             raft.Node // raft backing for the commit/error channel
	memoryStorage    *raft.MemoryStorage
	wal              *wal.WAL
	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready
	snapCount        uint64
	transport        *rafthttp.Transport
	stopc            chan struct{} // signals proposal channel closed
	httpstopc        chan struct{} // signals http server to shutdown
	httpdonec        chan struct{} // signals http server shutdown complete
	mu               sync.RWMutex
	kvStore          map[string]string // current committed key-value pairs
}

func (rc *RaftNode) NewMembers(advertise, cluster string) error {
	urlsmap := strings.Split(cluster, ",")
	if len(urlsmap)%2 == 0 {
		return fmt.Errorf("Only odd number of manage are supported!")
	}
	found := false
	for _, urls := range urlsmap {
		m := &Member{
			Urls: urls,
		}
		var b []byte
		b = append(b, []byte(urls)...)
		hash := sha1.Sum(b)
		m.ID = binary.BigEndian.Uint64(hash[:8])

		if _, ok := rc.members[m.ID]; ok {
			return fmt.Errorf("member exists with identical ID %v", m)
		}
		if uint64(m.ID) == raft.None {
			return fmt.Errorf("cannot use %x as member id", raft.None)
		}
		if advertise == urls {
			rc.id = m.ID
			found = true
		}
		rc.members[m.ID] = m
	}
	if !found {
		return fmt.Errorf("Not found advertise urls")
	}

	return nil
}

// NewRaftNode initiates a raft
func newRaftNode(
	advertise string,
	cluster string,
	join bool,
	metaDir string) (*RaftNode, error) {

	if _, err := os.Stat(metaDir); err != nil {
		if !os.IsExist(err) {
			glog.Fatalf("raft meta dir %s not exist\n", metaDir)
		}
	}

	rc := &RaftNode{
		members:          make(map[uint64]*Member),
		join:             join,
		proposeC:         make(chan string),
		confChangeC:      make(chan raftpb.ConfChange),
		commitC:          make(chan *string),
		errorC:           make(chan error),
		stopc:            make(chan struct{}),
		httpstopc:        make(chan struct{}),
		httpdonec:        make(chan struct{}),
		snapCount:        defaultSnapshotCount,
		snapshotterReady: make(chan *snap.Snapshotter, 1),
	}
	if err := rc.NewMembers(advertise, cluster); err != nil {
		return nil, err
	}
	rc.waldir = fmt.Sprintf("%s/wal-%d", metaDir, rc.id)
	rc.snapdir = fmt.Sprintf("%s/snapshot-%d", metaDir, rc.id)

	go rc.startRaft()
	go rc.readCommits(rc.commitC, rc.errorC)

	return rc, nil
}

// stop closes http, closes all channels, and stops raft.
func (rc *RaftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}


func (rc *RaftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *RaftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}



func (rc *RaftNode) serveRaft() {
	url, err := url.Parse(rc.members[rc.id].Urls)
	if err != nil {
		glog.Fatalf("raftnode: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		glog.Fatalf("raftnode: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		glog.Fatalf("raftnode: Failed to serve rafthttp (%v)", err)
	}

	close(rc.httpdonec)
}


// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *RaftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					glog.Info("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}

		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *RaftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		glog.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

func (rc *RaftNode) serveChannels() {
	snap, err := rc.memoryStorage.Snapshot()
	if err != nil {
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.saveSnap(rd.Snapshot)
				rc.memoryStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.memoryStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot()
			rc.node.Advance()

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}


// startRaft 开始RAFT
func (rc *RaftNode) startRaft() {
	if !FileExist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			glog.Fatalf("raft: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	oldwal := FileExist(rc.waldir)
	rc.wal = rc.replayWAL()
	var rpeers []raft.Peer
	for id := range rc.members {
		rpeers = append(rpeers, raft.Peer{ID: id})
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              ElectionTickTime,
		HeartbeatTick:             HeartbeatTickTime,
		Storage:                   rc.memoryStorage,
		MaxSizePerMsg:             MaxSizePerMsg,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}
	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(fmt.Sprint("%ul", rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()

	for _, mem := range rc.members {
		rc.transport.AddPeer(types.ID(mem.ID), []string{mem.Urls})
	}

	go rc.serveRaft()
	go rc.serveChannels()
}
