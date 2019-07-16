package topology

import (
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/etcdserver/api/snap"
)

type Topology struct {
	NodeImpl
	mu              sync.RWMutex
	collectionMap   *ConcurrentReadMap
	chanFullVolumes chan VolumeInfo
	volumeSizeLimit uint64
	RNode           *RaftNode
}

func NewTopology(volumeSizeLimit uint64, node *RaftNode) *Topology {
	t := &Topology{RNode: node}
	t.collectionMap = NewConcurrentReadMap()
	t.id = NodeId("topo")
	t.volumeSizeLimit = volumeSizeLimit
	t.NodeImpl.value = t
	t.children = make(map[NodeId]Node)
	t.chanFullVolumes = make(chan VolumeInfo)
	// replay log
//	t.readCommits()
	// read commits from raft into kvStore map until error
	go t.readCommits()
	return t
}

func (s *Topology) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return "", true
}

func (s *Topology) Propose(k string, v string) {
	//	var buf bytes.Buffer
	/*
		if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
			log.Fatal(err)
		}
	*/
	//	s.proposeC <- buf.String()
}

func (t *Topology) readCommits() {
	for data := range t.RNode.ReadCommitC() {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapsho
			snapshot, err := t.RNode.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := t.recoverFromSnapshot(snapshot.Data); err != nil {
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

		t.mu.Lock()

		// TODO
		t.mu.Unlock()
	}
	if err, ok := <-t.RNode.ReadErrorC(); ok {
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

// IsLeader is Leader
func (t *Topology) IsLeader() bool {

	return false
}
