package store

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type regionCfg struct {
	id       uint64   // local repalic id
	regionid uint64   // region id
	peers    []string // peers
	join     bool
}

type kv struct {
	Key   []byte
	Val   []byte
	IsDel bool
}

type regionStore struct {
	sync.RWMutex
	id       uint64        // region id
	proposeC chan<- string // channel for proposing updates

	confChangeC chan<- raftpb.ConfChange

	appliedIndex uint64 // applied log index

	checkPointIndex uint64 // write checkpoint index
	checkPointKey   []byte
	ps              *pebbleStore
	db              *pebbleDB
}

const CHECKPOINTER_PREFIX = "checkpoint:"

func newRegionStore(cfg *regionCfg, ps *pebbleStore) *regionStore {
	fmt.Printf("new regionstore %v ", cfg.regionid)
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)

	id := cfg.regionid

	rs := &regionStore{id: id, proposeC: proposeC, ps: ps, db: ps.db}

	rs.checkPointKey = []byte(CHECKPOINTER_PREFIX + strconv.FormatUint(id, 10))

	checkPointIndex, _ := rs.db.Get(rs.checkPointKey)

	if checkPointIndex != nil {
		rs.checkPointIndex = binary.BigEndian.Uint64(checkPointIndex)
	}

	rs.appliedIndex = rs.checkPointIndex

	ps.regionMap[id] = rs

	commitC, errorC := newRaftNode(int(cfg.id), cfg.regionid, cfg.peers,
		cfg.join, ps, proposeC, confChangeC)

	// read commits from raft into ps until error
	go rs.readCommits(commitC, errorC)

	return rs
}

func (rs *regionStore) Get(k []byte) ([]byte, error) {

	key := rs.MakeRegionKey(k)
	value, err := rs.db.Get(key)

	return value, err
}

func (rs *regionStore) Set(k []byte, v []byte) {
	var buf bytes.Buffer

	if err := gob.NewEncoder(&buf).Encode(kv{Key: k, Val: v}); err != nil {
		log.Fatal(err)
	}

	// maybe we need timeout err
	rs.proposeC <- buf.String()
}

func (rs *regionStore) Delete(k []byte) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{Key: k, IsDel: true}); err != nil {
		log.Fatal(err)
	}
	rs.proposeC <- buf.String()

	return nil
}

func (rs *regionStore) Close() {
	if rs.proposeC != nil {
		close(rs.proposeC)
	}
	if rs.confChangeC != nil {
		close(rs.confChangeC)
	}
}

// TODO refine
func GetRegionIdByKey(key []byte) uint64 {
	// mybe we shuld hash then use bits
	// we use second 4 bit as region id
	//return uint64(key[0] & 0x0F)
	// test data is uint64 first byte will always 0, so use last byte

	//return uint64(key[len(key)-1] & 0x01)  // 2 regions
	return 0
}

func (rs *regionStore) MakeRegionKey(key []byte) []byte {
	k := make([]byte, 8, 8+len(key))
	//regionid := GetRegionIdByKey(key)
	regionid := rs.id
	//binary.LittleEndian.PutUint64(k, regionid)
	binary.BigEndian.PutUint64(k, regionid)
	k = append(k, key...)

	return k
}

func (rs *regionStore) readCommits(commitC <-chan *commit, errorC <-chan error) {
	p := rs.db
	for commit := range commitC {
		if commit.snapshot != nil {
			// signaled to load snapshot
			snapshot := commit.snapshot
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := rs.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
				rs.appliedIndex = commit.index
				// write checkpoint
				rs.writeCheckpoint(true)
			} else {
				log.Panic("can't find snapshot")
			}
			continue
		}

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("regionStore: could not decode message (%v)", err)
			}

			// TODO refine
			key := rs.MakeRegionKey(dataKv.Key)
			if dataKv.IsDel {
				p.NoSyncDelete(key)
			} else {
				p.NoSyncSet(key, []byte(dataKv.Val))
			}

		}
		close(commit.applyDoneC)
		// update checkpointer but keep in memory wait to write to pebble.DB
		// maybe old appliedindex + 1 less then commit.index
		// due to commit.index max log entry rather then normal log entry
		rs.appliedIndex = commit.index
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (rs *regionStore) getSnapshot() ([]byte, uint64, error) {
	iter, appliedIndex, err := rs.getSnapshortIter()
	if err != nil {
		log.Fatal(err)
	}

	data := map[string]string{}
	for iter.First(); iter.Valid(); iter.Next() {
		data[string(iter.Key())] = string(iter.Value())
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	snapData, _ := json.Marshal(data)

	return snapData, appliedIndex, nil
}

func (rs *regionStore) getSnapshortIter() (*pebble.Iterator, uint64, error) {
	rs.RLock()
	defer rs.RUnlock()
	appliedIndex := rs.appliedIndex
	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, rs.id)

	iter, err := rs.db.PrefixScanIterator(prefix)

	if err != nil {
		log.Fatal(err)
	}

	return iter, appliedIndex, nil
}

func (rs *regionStore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}

	for k, v := range store {
		rs.db.NoSyncSet([]byte(k), []byte(v))
	}

	return nil
}

func (rs *regionStore) writeCheckpointNoForce() error {
	return rs.writeCheckpoint(false)
}

func (rs *regionStore) writeCheckpoint(force bool) error {

	id := rs.id
	checkpoint := rs.checkPointIndex
	val := rs.appliedIndex

	if force || val > checkpoint {
		// special key
		key := []byte(CHECKPOINTER_PREFIX + strconv.FormatUint(id, 10))
		//value := []byte(strconv.FormatUint(val, 10))

		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, val)

		err := rs.db.SyncSet(key, value)
		if err != nil {
			log.Fatal(err)
		}
		//log.Printf("regionStore %v write checkpoint %v succ", id, val)
		rs.checkPointIndex = val
	}
	return nil
}

func (rs *regionStore) getCheckpoint() (uint64, error) {

	id := rs.id
	key := []byte(CHECKPOINTER_PREFIX + strconv.FormatUint(id, 10))

	val, _ := rs.db.Get(key)

	checkpoint := uint64(0)
	if val != nil {
		//checkpoint, err := strconv.ParseUint(string(val), 10)
		checkpoint = binary.BigEndian.Uint64(val)
		log.Printf("Get checkpoint from storage %v", checkpoint)
	}
	return checkpoint, nil
}
