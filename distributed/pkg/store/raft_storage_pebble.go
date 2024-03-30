package store

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"go.etcd.io/etcd/raft/v3"
	pb "go.etcd.io/etcd/raft/v3/raftpb"
)

type pebbleRaftStorage struct {
	sync.Mutex

	id uint64    //region id
	db *pebbleDB //logdb
	rs *regionStore

	// TODO should load form disk instead of memeroy
	hardState        pb.HardState
	snapshotMetadata pb.SnapshotMetadata
	// ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry

	hardStateKey []byte
	confStateKey []byte
}

var hardStateKeyPrefix = "hardState-%s"
var confStateKeyPrefix = "confState-%s"

// NewPebbleRaftStorage creates an empty pebbleRaftStorage.
func NewPebbleRaftStorage(id uint64, ps *pebbleStore) *pebbleRaftStorage {

	// TODO reconstruct hardState, snapshot, ents
	// Get from db  key : dummy entry for region
	// if nil ,is zero index

	// 1. get checkpoint
	// we promise rs is not nil
	rs := ps.regionMap[id]

	db := ps.logdb

	ents := make([]pb.Entry, 1)

	// reload wal log from checkpoint
	checkpoint, _ := rs.getCheckpoint()
	log.Printf("checkpoint: %v", checkpoint)
	// hack for init due to log entry start with 1
	if checkpoint == 0 {
		checkpoint = 1
	}

	index := checkpoint
	if checkpoint > 0 {
		key := MakeEntryKey(id, checkpoint)
		val, _ := db.Get(key)

		if val != nil {
			ents = make([]pb.Entry, 0)
		} else {
			fmt.Printf("no log entry from checkpoint %v \n", checkpoint)
		}

		// get all log entry until not find
		// TODO refine use GE scan
		for val != nil {
			entry := decodeLogEntry(val)
			fmt.Printf("Get entry term %v index %v \n", entry.Term, entry.Index)
			ents = append(ents, *entry)
			index = index + 1
			key = MakeEntryKey(id, index)
			val, _ = db.Get(key)
		}

	}

	hardStateKey := []byte(fmt.Sprintf(hardStateKeyPrefix, id))
	confStateKey := []byte(fmt.Sprintf(confStateKeyPrefix, id))

	prs := &pebbleRaftStorage{
		id: id,
		// When starting from scratch populate the list with a dummy entry at term zero.
		ents: ents,
		db:   ps.logdb,
		rs:   rs,

		hardStateKey: hardStateKey,
		confStateKey: confStateKey,
	}

	// maybe we should put into InitialState()
	hs, err := db.Get(prs.hardStateKey)
	if hs != nil {
		err = prs.hardState.Unmarshal(hs)
		log.Printf("load hardState %v \n", prs.hardState.String())
		//log.Printf("load hardState2 term %v  commit %v", prs.hardState.Term, prs.hardState.Commit)

		//var hardstate pb.HardState
		//hardstate.Unmarshal(hs)
		//log.Printf("load hardState3 %v", hardstate.String())
		//log.Printf("load hardState4 term %v  commit %v", hardstate.Term, hardstate.Commit)

		if err != nil {
			log.Fatal(err)
		}
	}

	cs, _ := prs.db.Get(prs.confStateKey)
	if cs != nil {
		err = prs.snapshotMetadata.ConfState.Unmarshal(cs)
		log.Printf("load confState %v \n", prs.snapshotMetadata.ConfState.String())
		//log.Printf("load confState learners %v", prs.snapshotMetadata.ConfState.Learners)

		if err != nil {
			log.Fatal(err)
		}
	}

	return prs
}

func (p *pebbleRaftStorage) Close() error {
	return nil
}

// InitialState returns the saved HardState and ConfState information.
func (p *pebbleRaftStorage) InitialState() (pb.HardState, pb.ConfState, error) {

	// TODO reconstruct hardState, snapshot, ents
	return p.hardState, p.snapshotMetadata.ConfState, nil
}

// Entries returns a slice of log entries in the range [lo,hi).
// MaxSize limits the total size of the log entries returned, but
// Entries returns at least one entry if any.
func (p *pebbleRaftStorage) Entries(lo uint64, hi uint64, maxSize uint64) ([]pb.Entry, error) {
	p.Lock()
	defer p.Unlock()

	offset := p.ents[0].Index
	if lo <= offset {
		return nil, raft.ErrCompacted
	}
	if hi > p.lastIndex()+1 {
		log.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, p.lastIndex())
	}
	// only contains dummy entries.
	if len(p.ents) == 1 {
		return nil, raft.ErrUnavailable
	}

	ents := p.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

// copy from  raft
func limitSize(ents []pb.Entry, maxSize uint64) []pb.Entry {
	if len(ents) == 0 {
		return ents
	}
	size := ents[0].Size()
	var limit int
	for limit = 1; limit < len(ents); limit++ {
		size += ents[limit].Size()
		if uint64(size) > maxSize {
			break
		}
	}
	return ents[:limit]
}

// Term returns the term of entry i, which must be in the range
// [FirstIndex()-1, LastIndex()]. The term of the entry before
// FirstIndex is retained for matching purposes even though the
// rest of that entry may not be available.
func (p *pebbleRaftStorage) Term(i uint64) (uint64, error) {
	p.Lock()
	defer p.Unlock()
	offset := p.ents[0].Index
	if i < offset {
		return 0, raft.ErrCompacted
	}
	if int(i-offset) >= len(p.ents) {
		return 0, raft.ErrUnavailable
	}
	return p.ents[i-offset].Term, nil
}

// LastIndex returns the index of the last entry in the log.
func (p *pebbleRaftStorage) LastIndex() (uint64, error) {
	p.Lock()
	defer p.Unlock()
	return p.lastIndex(), nil

}

func (p *pebbleRaftStorage) lastIndex() uint64 {
	return p.ents[0].Index + uint64(len(p.ents)) - 1
}

// FirstIndex returns the index of the first log entry that is
// possibly available via Entries (older entries have been incorporated
// into the latest Snapshot; if storage only contains the dummy entry the
// first log entry is not available).
func (p *pebbleRaftStorage) FirstIndex() (uint64, error) {
	p.Lock()
	defer p.Unlock()
	return p.firstIndex(), nil
}

func (p *pebbleRaftStorage) firstIndex() uint64 {
	return p.ents[0].Index + 1
}

// Snapshot returns the most recent snapshot.
// If snapshot is temporarily unavailable, it should return ErrSnapshotTemporarilyUnavailable,
// so raft state machine could know that Storage needs some time to prepare
// snapshot and call Snapshot later.
func (p *pebbleRaftStorage) Snapshot() (pb.Snapshot, error) {
	rs := p.rs

	snapData, appliedIndex, _ := rs.getSnapshot()
	term, err := p.Term(appliedIndex)
	if err != nil {
		return pb.Snapshot{}, err
	}

	return pb.Snapshot{
		Metadata: pb.SnapshotMetadata{
			ConfState: p.snapshotMetadata.ConfState,
			Index:     appliedIndex,
			Term:      term,
		},
		Data: snapData,
	}, nil
}

func MakeEntryKey(id uint64, index uint64) []byte {
	key := make([]byte, 16)
	binary.BigEndian.PutUint64(key, id)
	binary.BigEndian.PutUint64(key[8:], index)

	return key
}

func (p *pebbleRaftStorage) encodeEntry(entry *pb.Entry) ([]byte, []byte, error) {
	// key region id + entry index
	key := MakeEntryKey(p.id, entry.Index)

	// value : entry
	value, err := entry.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	return key, value, nil
}

func decodeLogEntry(data []byte) *pb.Entry {
	var entry pb.Entry
	entry.Unmarshal(data)

	return &entry
}

func (p *pebbleRaftStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	p.Lock()
	defer p.Unlock()

	first := p.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return nil
	}
	// truncate compacted entries
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - p.ents[0].Index
	switch {
	case uint64(len(p.ents)) > offset:
		// discard more than offset log entry
		p.ents = append([]pb.Entry{}, p.ents[:offset]...)
		p.ents = append(p.ents, entries...)
	case uint64(len(p.ents)) == offset:
		p.ents = append(p.ents, entries...)
	default:
		log.Panicf("missing log entry [last: %d, append at: %d]",
			p.lastIndex(), entries[0].Index)
	}

	// we shuld not store log entry, if we use raft WAL record log
	// but for support multi raft, we self implemnts WAL
	// write log to pebbleDB store
	li := len(entries) - 1
	//li := len(entries)
	for _, entry := range entries[:li] {
		key, value, err := p.encodeEntry(&entry)
		err = p.db.NoSyncSet(key, value)
		if err != nil {
			log.Fatal(err)
		}
		//fmt.Printf("no sync save log entry %v \n", entry.String())
	}

	// last one entry flush
	key, value, err := p.encodeEntry(&entries[li])
	err = p.db.SyncSet(key, value)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("append log to %v total add new log entry %v \n", last, li)
	//fmt.Printf("save log entry %v \n", entries[li].String())
	return nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
func (p *pebbleRaftStorage) Compact(compactIndex uint64) error {
	// range delete log
	startkey := MakeEntryKey(p.id, 0)
	endkey := MakeEntryKey(p.id, compactIndex)
	p.db.NoSyncDeleteRange(startkey, endkey)

	p.Lock()
	defer p.Unlock()
	offset := p.ents[0].Index
	// maybe is snpshot
	if compactIndex <= offset {
		return nil // aft.ErrCompacted
	}

	if compactIndex > p.lastIndex() {
		log.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, p.lastIndex())
	}

	i := compactIndex - offset
	ents := make([]pb.Entry, 1, 1+uint64(len(p.ents))-i)
	ents[0].Index = p.ents[i].Index
	ents[0].Term = p.ents[i].Term
	ents = append(ents, p.ents[i+1:]...)
	p.ents = ents

	return nil
}

// snapshot

/*
// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
func (p *pebbleRaftStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	p.Lock()
	defer p.Unlock()
	if i <= p.snapshotMetadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := p.ents[0].Index
	if i > p.lastIndex() {
		log.Panicf("snapshot %d is out of bound lastindex(%d)", i, p.lastIndex())
	}

	p.snapshotMetadata.Index = i
	p.snapshotMetadata.Term = p.ents[i-offset].Term
	if cs != nil {
		p.snapshotMetadata.ConfState = *cs
	}

	return pb.Snapshot{Matadata: p.snapshotMetadata, Data: data}, nil
}
*/

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
func (p *pebbleRaftStorage) ApplySnapshot(snap pb.Snapshot) error {
	p.Lock()
	defer p.Unlock()

	//handle check for old snapshot being applied
	msIndex := p.snapshotMetadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		return raft.ErrSnapOutOfDate
	}

	p.snapshotMetadata = snap.Metadata
	p.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}

	return nil
}

func (p *pebbleRaftStorage) isMustSync(entries []pb.Entry) bool {
	if len(entries) == 0 {
		return true
	}

	first := p.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	if last < first {
		return true
	}

	return false
}

// SetHardState saves the current HardState.
func (p *pebbleRaftStorage) SetHardState(st pb.HardState, entries []pb.Entry) error {
	// ignore emptry
	if st.Term == 0 {
		return nil
	}

	ismustsync := p.isMustSync(entries)

	p.Lock()
	defer p.Unlock()

	if p.hardState.Commit != st.Commit || p.hardState.Term != st.Term || p.hardState.Vote != st.Vote {
		p.hardState = st
		log.Printf("write hardstate %s", p.hardState.String())
		// write to pebbleDB
		hsData, err := st.Marshal()
		if err != nil {
			log.Fatal(err)
		}
		if ismustsync {
			p.db.SyncSet(p.hardStateKey, hsData)
		} else {
			p.db.NoSyncSet(p.hardStateKey, hsData)
		}
		//p.db.SyncSet(p.hardStateKey, hsData)
	}

	return nil
}

func (p *pebbleRaftStorage) SetConfState(cs *pb.ConfState) error {
	p.Lock()
	defer p.Unlock()

	// no equial function
	/*
		if cs.Equivalent(pb.ConfState{}) || cs.Equivalent(p.snapshotMetadata.ConfState) {
			return nil
		}
	*/

	p.snapshotMetadata.ConfState = *cs
	fmt.Printf("write conf state %v \n", cs.String())
	csData, err := cs.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	p.db.SyncSet(p.confStateKey, csData)

	return nil
}
