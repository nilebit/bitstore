package topology

import (
	"bytes"
	"encoding/json"
	"github.com/golang/glog"
	"github.com/nilebit/bitstore/pb"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc"
	"log"
	"net"
	"strconv"
	"sync"
)

type Topology struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	snapshotter *snap.Snapshotter
	NodeImpl
	collectionMap *ConcurrentReadMap
	chanFullVolumes chan VolumeInfo
	volumeSizeLimit uint64
	ConfChangeC chan<- raftpb.ConfChange
}

/*
func (t *Topology) GetVolumeLayout(collectionName string, rp *replicate.Placement, ttl *ttl.TTL) *VolumeLayout {
	return t.collectionMap.Get(collectionName, func() interface{} {
		return NewCollection(collectionName, t.volumeSizeLimit)
	}).(*Collection).GetOrCreateVolumeLayout(rp, ttl)
}

func (t *Topology) UnRegisterDataNode(dn *DataNode) {
	for _, v := range dn.GetVolumes() {
		glog.V(0).Infoln("Removing Volume", v.Id, "from the dead volume server", dn.Id())
		vl := t.GetVolumeLayout(v.Collection, v.ReplicaPlacement, v.Ttl)
		vl.SetVolumeUnavailable(dn, v.Id)
	}
	dn.UpAdjustVolumeCountDelta(-dn.GetVolumeCount())
	dn.UpAdjustActiveVolumeCountDelta(-dn.GetActiveVolumeCount())
	dn.UpAdjustMaxVolumeCountDelta(-dn.GetMaxVolumeCount())
	dn.Parent().UnlinkChildNode(dn.Id())
}
*/
func (t *Topology) SendHeartbeat(stream pb.Seaweed_SendHeartbeatServer) error {
	var dn *DataNode
	for {
		_, err := stream.Recv()
		if err != nil {
			if dn != nil {
				glog.V(0).Infof("lost disk node server %s:%d", dn.Ip, dn.Port)
		//		t.UnRegisterDataNode(dn)
			}
			return err
		}

		if dn == nil {

		}
	}
}

func (topo *Topology)StartServer( port int) error {
	address := ":" + strconv.Itoa(port)
	lis, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return err
	}
	s := grpc.NewServer()
	pb.RegisterSeaweedServer(s, topo)
	if err := s.Serve(lis); err != nil {
		return err
	}

	return nil
}

func NewTopology(volumeSizeLimit uint64,
	snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *Topology {
	t := &Topology{proposeC: proposeC, snapshotter: snapshotter}
	t.collectionMap = NewConcurrentReadMap()
	t.id = NodeId("topo")
	t.volumeSizeLimit = volumeSizeLimit
	t.NodeImpl.value = t
	t.children = make(map[NodeId]Node)
	t.chanFullVolumes = make(chan VolumeInfo)
	// replay log into key-value map
	t.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go t.readCommits(commitC, errorC)
	return t
}

func (s *Topology) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return "", true
}

func (s *Topology) Propose(k string, v string) {
	var buf bytes.Buffer
	/*
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	*/
	s.proposeC <- buf.String()
}

func (s *Topology) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapsho
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		// TODO
		/*
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		*/

		s.mu.Lock()

		// TODO
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *Topology) GetSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// TODO
	return json.Marshal(nil)
}

func (s *Topology) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// TODO
	return nil
}
