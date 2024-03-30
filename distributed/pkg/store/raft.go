package store

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"

	"go.uber.org/zap"
)

type commit struct {
	index      uint64
	data       []string
	applyDoneC chan<- struct{}
	snapshot   *raftpb.Snapshot
}

type raftNode struct {
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *commit           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id        int      // raft group member ID
	clusterid uint64   // region id, raft group id
	peers     []string // raft peer URLs
	join      bool     // node is joining an existing cluster

	confState    raftpb.ConfState
	compactIndex uint64
	appliedIndex uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *pebbleRaftStorage

	transport *rafthttp.Transport

	ps *pebbleStore

	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete

	logger *zap.Logger
}

var defaultSnapshotCount uint64 = 10000

func newRaftNode(id int, clusterid uint64, peers []string, join bool, ps *pebbleStore,
	proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *commit, <-chan error) {

	commitC := make(chan *commit)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,

		id:        id,
		clusterid: clusterid,
		peers:     peers,
		join:      join,

		stopc:     make(chan struct{}),
		httpstopc: make(chan struct{}),
		httpdonec: make(chan struct{}),

		ps: ps,

		logger: zap.NewExample(),

		// rest of structure populated after WAL replay
	}
	go rc.startRaft()
	return commitC, errorC
}

func (rc *raftNode) startRaft() {

	oldwal, _ := rc.replayWAL()

	/*
		snap, err := rc.raftStorage.Snapshot()
		if err != nil {
			panic(err)
		}

		rc.confState = snap.Metadata.ConfState
		rc.compactIndex = snap.Metadata.Index
		rc.appliedIndex = snap.Metadata.Index
	*/

	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldwal || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger,
		ID:          types.ID(rc.id),
		ClusterID:   types.ID(rc.clusterid),
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}

	go rc.serveRaft()
	go rc.serveChannels()

}

// listen stop
func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftenode: Failed parsing URL (%v)", err)
	}

	// Graceful shutdown
	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftnode: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftenode: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

// handle channel msg events
func (rc *raftNode) serveChannels() {

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
					fmt.Printf("propseC set nil\n")
				} else {
					// blocks until accepted by raft state machine
					//fmt.Printf("get msg from propseC\n")
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
			//rc.wal.Save(rd.HardState, rd.Entries)
			rc.raftStorage.SetHardState(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			// first send then append
			rc.transport.Send(rd.Messages) //should aysnc send
			rc.raftStorage.Append(rd.Entries)
			//rc.transport.Send(rd.Messages)
			applyDoneC, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerCompact(applyDoneC)
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

// WAL

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() (bool, error) {
	log.Printf("replaying WAL of member %d region %d \n", rc.id, rc.clusterid)

	rc.raftStorage = NewPebbleRaftStorage(rc.clusterid, rc.ps)

	// redo log from checkpoint index to commit index
	firstIndex, _ := rc.raftStorage.FirstIndex()
	lastIndex, _ := rc.raftStorage.LastIndex()

	// now old wal
	if lastIndex == 0 {
		log.Printf("no wal log")
		return false, nil
	}

	rc.confState = rc.raftStorage.snapshotMetadata.ConfState
	rc.compactIndex = firstIndex - 1
	rc.appliedIndex = firstIndex - 1

	// redo data by Ready()
	/*
		commitIndex := rc.raftStorage.hardState.Commit
		ents, _ := rc.raftStorage.Entries(firstIndex, commitIndex+1, maxCatchUpEntriesN)

		applyC, ok := rc.publishEntries(ents)

		if !ok {
			rc.stop()
		}

		if applyC != nil {
			<-applyC
		}

	*/
	return true, nil
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
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

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) (<-chan struct{}, bool) {
	if len(ents) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(ents))
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.confState = *rc.node.ApplyConfChange(cc)
			rc.raftStorage.SetConfState(&rc.confState)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return nil, false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
	}

	var applyDoneC chan struct{}

	if len(data) > 0 {
		applyDoneC = make(chan struct{}, 1)
		select {
		case rc.commitC <- &commit{index: ents[len(ents)-1].Index, data: data, applyDoneC: applyDoneC}:
		case <-rc.stopc:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	rc.appliedIndex = ents[len(ents)-1].Index

	return applyDoneC, true
}

// snapshot

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	snapshotIndex := snapshotToSave.Metadata.Index
	log.Printf("publishing snapshot at index %d", snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}

	// TODO refine snapshot save
	// here is danger lost snapshot when crash happened
	// cause we will retry get snapshot
	rc.commitC <- &commit{index: snapshotToSave.Metadata.Index, snapshot: &snapshotToSave}

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.raftStorage.SetConfState(&rc.confState)
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var maxCatchUpEntriesN uint64 = 10000

// compact log entry
func (rc *raftNode) maybeTriggerCompact(applyDoneC <-chan struct{}) {
	if rc.appliedIndex-rc.compactIndex <= maxCatchUpEntriesN {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneC != nil {
		select {
		case <-applyDoneC:
		case <-rc.stopc:
			return
		}
	}

	log.Printf("start compact [applied index: %d | last compact index: %d]", rc.appliedIndex, rc.compactIndex)

	compactIndex := rc.appliedIndex
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.compactIndex = compactIndex
}

// stop
// exit raft node
func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

// implement Raft interface
func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *raftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}
