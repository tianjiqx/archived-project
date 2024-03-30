package store

import (
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/matrixorigin/talent-challenge/matrixbase/distributed/pkg/cfg"
)

//const FLUSH_BATCH_INTERVAL = 100 * time.Microsecond //time.Millisecond
const FLUSH_BATCH_INTERVAL = 100 * time.Millisecond
const MAX_BATCH_SZIE = 100

type pebbleStore struct {
	sync.RWMutex
	db    *pebbleDB
	logdb *pebbleDB //or metadata db

	regionMap map[uint64](*regionStore)

	timer *time.Ticker

	//name string
}

// write checkpoint, then flush
func (s *pebbleStore) writeCheckpoints() error {
	for _, rs := range s.regionMap {
		rs.writeCheckpointNoForce()
	}
	return nil
}

// hack test init
var nameIdMap = map[string]uint64{"node1:8080": 1,
	"node2:8080":     2,
	"node3:8080":     3,
	"127.0.0.1:8080": 1,
}

func newPebbleStore(cfg cfg.StoreCfg, name string, peers [][]string) (Store, error) {
	pebbleDb, err := pebble.Open(cfg.DataPath+"/data", &pebble.Options{})
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	logdb, err2 := pebble.Open(cfg.DataPath+"/log", &pebble.Options{})
	if err2 != nil {
		log.Fatal(err2)
		return nil, err2
	}

	rm := map[uint64]*regionStore{}

	t := time.NewTicker(FLUSH_BATCH_INTERVAL)

	s := &pebbleStore{db: &pebbleDB{pebbleDb}, logdb: &pebbleDB{logdb}, regionMap: rm, timer: t}

	s.createRegion(0, nameIdMap[name], peers[0])
	//s.createRegion(1, nameIdMap[name], peers[1])

	// start task
	go func() {
		for {
			select {
			case <-t.C:
				s.writeCheckpoints()

				//s.flush()
				//fmt.Printf("flush current time: %v seq num %v \n", time.Now(), s.seq.num)
			}
		} // end for
	}()

	return s, nil
}

// TODO refine
// we should know all cluster node info and region distribution,
// then choice some peers generate new raft group

func (s *pebbleStore) createRegion(rid uint64, id uint64, peers []string) *regionStore {
	s.Lock()
	defer s.Unlock()

	// retry check
	if s.regionMap[rid] != nil {
		return s.regionMap[rid]
	}
	regCfg := &regionCfg{id: id, regionid: rid, peers: peers}
	region := newRegionStore(regCfg, s)

	return region
}

func (s *pebbleStore) Set(key []byte, value []byte) error {

	// write to proc
	rid := GetRegionIdByKey(key)
	/*
		if s.regionMap[rid] == nil {
			// new region
			s.createRegion(rid)
		}

	*/
	s.regionMap[rid].Set(key, value)

	//return s.SyncSet(key, value)
	return nil
}

func (s *pebbleStore) Get(key []byte) ([]byte, error) {
	rid := GetRegionIdByKey(key)
	/*
		if s.regionMap[rid] == nil {
			// new region
			s.createRegion(rid)
		}
	*/

	value, err := s.regionMap[rid].Get(key)

	return value, err
}

func (s *pebbleStore) Delete(key []byte) error {
	// write to proc
	rid := GetRegionIdByKey(key)

	err := s.regionMap[rid].Delete(key)

	return err
}

func (s *pebbleStore) Close() error {
	if err := s.db.Close(); err != nil {
		log.Fatal(err)
	}

	if err := s.logdb.Close(); err != nil {
		log.Fatal(err)
	}

	for _, region := range s.regionMap {
		region.Close()
	}
	//s.timer.Stop()
	return nil
}
