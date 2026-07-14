package kvraft

import (
	"bytes"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Value struct {
	value   string
	version rpc.Tversion
}

type SnapshotValue struct {
	Value   string
	Version rpc.Tversion
}

type KVSnapshot struct {
	KVStore map[string]SnapshotValue
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.

	mu      sync.Mutex
	kvStore map[string]Value
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// Your code here
	getArgs, getOk := req.(rpc.GetArgs)

	getReply := rpc.GetReply{}

	if getOk {
		val, ok := kv.kvStore[getArgs.Key]
		if ok {
			getReply.Value = val.value
			getReply.Version = val.version
			getReply.Err = rpc.OK
			return getReply
		} else {
			getReply.Err = rpc.ErrNoKey
			return getReply
		}
	}

	putReply := rpc.PutReply{}
	putArgs, putOk := req.(rpc.PutArgs)
	if putOk {
		val, ok := kv.kvStore[putArgs.Key]

		if ok {
			if val.version != putArgs.Version {
				putReply.Err = rpc.ErrVersion
				return putReply
			}

			kv.kvStore[putArgs.Key] = Value{value: putArgs.Value, version: putArgs.Version + 1}
		} else {
			if putArgs.Version > 0 {
				putReply.Err = rpc.ErrNoKey
				return putReply
			}
			kv.kvStore[putArgs.Key] = Value{value: putArgs.Value, version: 1}
		}
		putReply.Err = rpc.OK
		return putReply
	}

	return nil
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	store := make(map[string]SnapshotValue)
	for key, val := range kv.kvStore {
		store[key] = SnapshotValue{Value: val.value, Version: val.version}
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(KVSnapshot{KVStore: store}); err != nil {
		log.Fatalf("could not encode kv snapshot: %v", err)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var snap KVSnapshot
	if err := d.Decode(&snap); err != nil {
		log.Fatalf("could not decode kv snapshot: %v", err)
	}

	store := make(map[string]Value)
	for key, val := range snap.KVStore {
		store[key] = Value{value: val.Value, version: val.Version}
	}

	kv.mu.Lock()
	kv.kvStore = store
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	err, res := kv.rsm.Submit(*args)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep, ok := res.(rpc.GetReply)

	if !ok {
		// do somethign here
	}

	reply.Err = rep.Err
	reply.Value = rep.Value
	reply.Version = rep.Version
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	err, res := kv.rsm.Submit(*args)

	if err != rpc.OK {
		reply.Err = err
		return
	}

	rep, ok := res.(rpc.PutReply)
	if !ok {
		// do something here
	}

	reply.Err = rep.Err

}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}
	kv.kvStore = make(map[string]Value)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
