package store

import (
	"log"

	"github.com/cockroachdb/pebble"
)

type pebbleDB struct {
	db *pebble.DB
}

func (s *pebbleDB) Get(key []byte) ([]byte, error) {

	value, closer, err := s.db.Get(key)

	if err == pebble.ErrNotFound {
		// not found
		return nil, nil
	}

	if err != nil {
		log.Fatal(err)
	} else {
		//fmt.Printf("Get %s value: %s\n", key, value)
	}

	if err := closer.Close(); err != nil {
		log.Fatal(err)
	}

	return value, err
}

// 1000  750ms
func (s *pebbleDB) SyncSet(key []byte, value []byte) error {
	err := s.db.Set(key, value, pebble.Sync)
	if err != nil {
		log.Fatal(err)
	}

	//fmt.Printf("Set key: %s value: %s\n", key, value)

	return err
}

// 1000 3.49ms
func (s *pebbleDB) NoSyncSet(key []byte, value []byte) error {
	err := s.db.Set(key, value, pebble.NoSync)
	if err != nil {
		log.Fatal(err)
	}

	//fmt.Printf("Set key: %s value: %s\n", key, value)

	return err
}

func (s *pebbleDB) SyncDelete(key []byte) error {
	// sync will write disk
	err := s.db.Delete(key, pebble.Sync)

	if err != nil {
		log.Fatal(err)
	} else {
		//fmt.Printf("Delete key %s \n", key)
	}

	return err
}

func (s *pebbleDB) NoSyncDelete(key []byte) error {
	// sync will write disk
	err := s.db.Delete(key, pebble.NoSync)

	if err != nil {
		log.Fatal(err)
	} else {
		//fmt.Printf("Delete key %s \n", key)
	}

	return err
}

func (s *pebbleDB) NoSyncDeleteRange(startkey []byte, endkey []byte) error {

	err := s.db.DeleteRange(startkey, endkey, pebble.NoSync)

	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (s *pebbleDB) Close() error {
	if err := s.db.Close(); err != nil {
		log.Fatal(err)
	}

	//s.timer.Stop()
	return nil
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil // no upper-bound
}

func (s *pebbleDB) PrefixScan(prefix []byte) (map[string]string, error) {

	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}

	iter := s.db.NewIter(prefixIterOptions(prefix))
	data := map[string]string{}
	for iter.First(); iter.Valid(); iter.Next() {
		//fmt.Printf("%s\n", iter.Key())
		data[string(iter.Key())] = string(iter.Value())
	}
	if err := iter.Close(); err != nil {
		log.Fatal(err)
	}

	return data, nil
}

func (s *pebbleDB) PrefixScanIterator(prefix []byte) (*pebble.Iterator, error) {

	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}

	iter := s.db.NewIter(prefixIterOptions(prefix))

	return iter, nil
}

/*

// batch seq
type seqNumber struct {
	sync.RWMutex
	refCount uint64
	num      uint64
}



// It is work, but single client test is meaningless
func (s *pebbleDB) flush() error {
	s.seq.Lock()
	defer s.seq.Unlock()

	//if s.seq.refCount > 0 {
	if true {
		// specila kv, just for flush pebble bacth
		key := []byte("tianjiqx-nop")
		value := []byte("nop")

		err := s.SyncSet(key, value)
		if err == nil {
			//atomic.StoreInt64(&s.seq.refCount, 0)
			s.seq.refCount = 0
			s.seq.num = s.seq.num + 1
		}
	}
	return nil
}


func (s *pebbleStore) Set(key []byte, value []byte) error {

	s.seq.RLock()
	curSeqNum := s.seq.num
	curRefCount := s.seq.refCount

	if curRefCount < MAX_BATCH_SZIE {
		err := s.NoSyncSet(key, value)
		if err != nil {
			s.seq.RUnlock()
			return err
		}
		atomic.AddInt64(&s.seq.refCount, 1)
		s.seq.RUnlock()
	} else {
		s.seq.RUnlock()
		s.seq.Lock()
		curSeqNum = s.seq.num
		curRefCount = s.seq.refCount
		if curRefCount >= MAX_BATCH_SZIE {
			err := s.SyncSet(key, value)
			if err == nil {
				s.seq.num = s.seq.num + 1
				curRefCount = 0
			}
			s.seq.Unlock()
			return err
		} else {
			err := s.NoSyncSet(key, value)
			if err != nil {
				s.seq.Unlock()
				return err
			}
			curRefCount = curRefCount + 1
		}
		s.seq.Unlock()
	}

	// wait flush
	for s.seq.num == curSeqNum {
		//time.Sleep(time.Millisecond)
		time.Sleep(time.Microsecond)
	}

	return nil
}
*/

/*
func (s *pebbleStore) Delete(key []byte) error {
	s.seq.RLock()
	curSeqNum := s.seq.num
	curRefCount := s.seq.refCount

	if curRefCount < MAX_BATCH_SZIE {
		err := s.NoSyncDelete(key)
		if err != nil {
			s.seq.RUnlock()
			return err
		}
		atomic.AddInt64(&s.seq.refCount, 1)
		s.seq.RUnlock()
	} else {
		s.seq.RUnlock()
		s.seq.Lock()
		curSeqNum = s.seq.num
		curRefCount = s.seq.refCount
		if curRefCount >= MAX_BATCH_SZIE {
			err := s.SyncDelete(key)
			if err == nil {
				s.seq.num = s.seq.num + 1
				curRefCount = 0
			}
			s.seq.Unlock()
			return err
		} else {
			err := s.NoSyncDelete(key)
			if err != nil {
				s.seq.Unlock()
				return err
			}
			curRefCount = curRefCount + 1
		}
		s.seq.Unlock()
	}

	// wait flush
	for s.seq.num == curSeqNum {
		//time.Sleep(time.Microsecond)
		time.Sleep(time.Microsecond)
	}

	return nil
}
*/
