package topology

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/nilebit/bitstore/util"
	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"
	"go.uber.org/zap"
)

const (
	// ElectionTickTime 选举超时
	ElectionTickTime int = 20
	// HeartbeatTickTime 心跳超时
	HeartbeatTickTime int = 5
	// MaxSizePerMsg 每包最大值
	MaxSizePerMsg uint64 = 4194304
)

type Member struct {
	ID uint64
	Urls string
}

// RaftNode A key-value stream backed by raft
type RaftNode struct {
	proposeC         chan string            // proposed messages (k,v)
	confChangeC      chan raftpb.ConfChange // proposed cluster config changes
	commitC          chan *string           // entries committed to log (k,v)
	errorC           chan error             // errors from raft session
	id               uint64                 // client ID for raft session
	members    	     map[uint64]*Member     // raft peer URLs
	join             bool                   // node is joining an existing cluster
	waldir           string                 // path to WAL directory
	snapdir          string                 // path to snapshot directory
	lastIndex        uint64                 // index of log at start
	confState        raftpb.ConfState
	snapshotIndex    uint64
	appliedIndex     uint64
	node             raft.Node // raft backing for the commit/error channel
	raftStorage      *raft.MemoryStorage
	wal              *wal.WAL
	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready
	getSnapshot      func() ([]byte, error)
	snapCount        uint64
	transport        *rafthttp.Transport
	stopc            chan struct{} // signals proposal channel closed
	httpstopc        chan struct{} // signals http server to shutdown
	httpdonec        chan struct{} // signals http server shutdown complete
}

func  (rc *RaftNode)NewMembers(advertise,cluster string) error {
	urlsmap := strings.Split(cluster, ",")
	if len(urlsmap)%2 == 0 {
		return fmt.Errorf("Only odd number of manage are supported!")
	}
	found := false
	for _, urls := range urlsmap {
		m := &Member{
			Urls:urls,
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
func NewRaftNode(
	advertise string,
	cluster string,
	join bool,
	metaDir string,
	getSnapshot func() ([]byte, error)) (*RaftNode, error) {

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
		getSnapshot:      getSnapshot,
		stopc:            make(chan struct{}),
		httpstopc:        make(chan struct{}),
		httpdonec:        make(chan struct{}),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
	}
	if err := rc.NewMembers(advertise, cluster); err != nil {
		return nil, err
	}
	rc.waldir = fmt.Sprintf("%s/wal-%d", metaDir,rc.id)
	rc.snapdir = fmt.Sprintf("%s/snapshot-%d", metaDir, rc.id)

	go rc.startRaft()
	return rc, nil
}

// FileExist checking file
func FileExist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// loadSnapshot 获取快照
func (rc *RaftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *RaftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *RaftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	return w
}

// startRaft 开始RAFT
func (rc *RaftNode) startRaft() {
	if !FileExist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			glog.Fatalf("raftnode: cannot create dir for snapshot (%v)", err)
		}
	}
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	oldwal := FileExist(rc.waldir)
	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.members))
	for _, mem := range rc.members {
		rpeers = append(rpeers, raft.Peer{ID: uint64(mem.ID)})
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              ElectionTickTime,
		HeartbeatTick:             HeartbeatTickTime,
		Storage:                   rc.raftStorage,
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

// serveRaft RAFT监控服务
func (rc *RaftNode) serveRaft() {
	url, err := url.Parse(rc.members[rc.id].Urls)
	if err != nil {
		log.Fatalf("raftnode: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftnode: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftnode: Failed to serve rafthttp (%v)", err)
	}

	close(rc.httpdonec)
}

func (rc *RaftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *RaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	rc.commitC <- nil // trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
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
					log.Println("I've been removed from the cluster! Shutting down.")
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
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// stop closes http, closes all channels, and stops raft.
func (rc *RaftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *RaftNode) maybeTriggerSnapshot() {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
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

// serveChannels 处理通道
func (rc *RaftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
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
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			//TODO
//			rc.maybeTriggerSnapshot()
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

// Process Raft node process
func (rc *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}

// IsIDRemoved removed
func (rc *RaftNode) IsIDRemoved(id uint64) bool {
	return false
}

// ReportUnreachable Report Unreachable
func (rc *RaftNode) ReportUnreachable(id uint64) {}

// ReportSnapshot Report Snapshot
func (rc *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}

// ReadCommitC Read commitC
func (rc *RaftNode) ReadCommitC() <-chan *string {
	return rc.commitC
}

// ReadErrorC Read ErrorC
func (rc *RaftNode) ReadErrorC() <-chan error {
	return rc.errorC
}

// ReadStatus Read node status
func (rc *RaftNode) ReadStatus() (stat util.ClusterStatusResult) {
	nodestatus := rc.node.Status()

	if nodestatus.Lead == uint64(rc.id) {
		stat.IsLeader = true
	}
	url, err := url.Parse(rc.members[nodestatus.Lead].Urls)
	if err == nil {
		stat.Leader = url.Host
	} else {
		log.Fatalf("raftnode: Failed parsing URL (%v)", err)
	}
	for _, mem := range rc.members {
		stat.Peers = append(stat.Peers, mem.Urls)
	}


	return stat
}
