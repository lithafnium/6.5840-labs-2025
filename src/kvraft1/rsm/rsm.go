package rsm

import (
	"bytes"
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me    int
	Epoch int64
	Req   any
	Id    int
}

type RequestId struct {
	Me    int
	Epoch int64
	Id    int
}

type SnapshotData struct {
	LastApplied int
	State       []byte
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.

	counter         int
	epoch           int64
	lastApplied     int
	submitListeners map[RequestId]chan any
}

func encodeSnapshot(lastApplied int, state []byte) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(SnapshotData{LastApplied: lastApplied, State: state}); err != nil {
		log.Fatalf("could not encode rsm snapshot: %v", err)
	}
	return w.Bytes()
}

func decodeSnapshot(data []byte) SnapshotData {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snap SnapshotData
	if err := d.Decode(&snap); err != nil {
		log.Fatalf("could not decode rsm snapshot: %v", err)
	}
	return snap
}

func (rsm *RSM) maybeSnapshot(index int) {
	if rsm.maxraftstate == -1 || rsm.rf.PersistBytes() <= rsm.maxraftstate {
		return
	}

	rsm.rf.Snapshot(index, encodeSnapshot(index, rsm.sm.Snapshot()))
}

func (rsm *RSM) read() {
	for msg := range rsm.applyCh {
		if msg.SnapshotValid {
			snap := decodeSnapshot(msg.Snapshot)
			rsm.mu.Lock()
			if snap.LastApplied <= rsm.lastApplied {
				rsm.mu.Unlock()
				continue
			}
			rsm.lastApplied = snap.LastApplied
			rsm.mu.Unlock()
			rsm.sm.Restore(snap.State)
			continue
		}

		if msg.CommandValid {
			rsm.mu.Lock()
			if msg.CommandIndex <= rsm.lastApplied {
				rsm.mu.Unlock()
				continue
			}
			rsm.lastApplied = msg.CommandIndex
			rsm.mu.Unlock()

			command := msg.Command
			op, ok := command.(Op)
			if !ok {
				// do something here
				continue
			}

			res := rsm.sm.DoOp(op.Req)

			if op.Me == rsm.me {
				key := RequestId{Me: op.Me, Epoch: op.Epoch, Id: op.Id}

				rsm.mu.Lock()
				ch, ok := rsm.submitListeners[key]
				if ok {
					delete(rsm.submitListeners, key)
				}
				rsm.mu.Unlock()

				if ok {
					// Buffered channel means the applier never blocks if Submit
					// has already decided to return ErrWrongLeader.
					ch <- res
				}
			}

			rsm.maybeSnapshot(msg.CommandIndex)
		}

	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:              me,
		maxraftstate:    maxraftstate,
		applyCh:         make(chan raftapi.ApplyMsg),
		sm:              sm,
		counter:         0,
		epoch:           time.Now().UnixNano(),
		submitListeners: make(map[RequestId]chan any),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	if data := persister.ReadSnapshot(); len(data) > 0 {
		snap := decodeSnapshot(data)
		rsm.lastApplied = snap.LastApplied
		rsm.sm.Restore(snap.State)
	}

	go rsm.read()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	rsm.mu.Lock()
	id := rsm.counter
	rsm.counter++
	key := RequestId{Me: rsm.me, Epoch: rsm.epoch, Id: id}

	listener := make(chan any, 1)
	rsm.submitListeners[key] = listener
	rsm.mu.Unlock()

	op := Op{Me: rsm.me, Epoch: key.Epoch, Id: id, Req: req}

	_, term, isLeader := rsm.rf.Start(op)

	if !isLeader {
		rsm.mu.Lock()
		delete(rsm.submitListeners, key)
		rsm.mu.Unlock()

		return rpc.ErrWrongLeader, nil
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	defer func() {
		rsm.mu.Lock()
		delete(rsm.submitListeners, key)
		rsm.mu.Unlock()
	}()

	for {
		select {
		case res := <-listener:
			return rpc.OK, res
		case <-ticker.C:
			currentTerm, stillLeader := rsm.rf.GetState()
			if !stillLeader || currentTerm != term {
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
