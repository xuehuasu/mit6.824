package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(cond bool, format string, a ...interface{}) (n int, err error) {
	if Debug && cond {
		log.Printf("server "+format, a...)
	}
	return
}

type Op struct {
	Op        string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastSnapshot int

	// Your definitions here.
	notify      map[int]chan bool
	db          map[string]string
	lastRequest map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	lastRequestId, ok := kv.lastRequest[args.ClientId]
	if ok && lastRequestId >= args.RequestId {
		reply.Err = OK
		reply.Value = kv.db[args.Key]
		// DPrintf(true, "Get %v MultOp %v", kv.me, args)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Op:        GET,
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ch := make(chan bool, 1)
	index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	// DPrintf(true, "Get %v args %v index %v term %v", kv.me, args, index, term)
	kv.notify[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		// DPrintf(true, "Get %v result args %v reply %v", kv.me, args, reply)
		delete(kv.notify, index)
		kv.mu.Unlock()
		close(ch)
	}()

	select {
	case <-ch:
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
			kv.mu.Lock()
			reply.Value = kv.db[args.Key]
			kv.mu.Unlock()
		}
	case <-time.After(3 * time.Second):
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = ErrTimeOut
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	lastRequestId, ok := kv.lastRequest[args.ClientId]
	kv.mu.Unlock()

	if ok && lastRequestId >= args.RequestId {
		reply.Err = OK
		return
	}

	op := Op{
		Op:        args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	ch := make(chan bool, 1)
	kv.mu.Lock()
	index, _, _ := kv.rf.Start(op)
	kv.notify[index] = ch
	// DPrintf(true, "PutAppend %v args %v index %v term %v", kv.me, args, index, term)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		// DPrintf(true, "PutAppend %v result args %v reply %v", kv.me, args, reply)
		delete(kv.notify, index)
		kv.mu.Unlock()
		close(ch)
	}()

	select {
	case <-ch:
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		} else {
			reply.Err = OK
		}
	case <-time.After(3 * time.Second):
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = ErrTimeOut
		return
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.mu.Lock()
			op, _ := msg.Command.(Op)

			lastRequestId, ok := kv.lastRequest[op.ClientId]
			if ok && lastRequestId >= op.RequestId {
				// DPrintf(true, "applier %d refuse %v", kv.me, msg)
				kv.mu.Unlock()
				continue
			}

			if op.Op == PUT {
				kv.db[op.Key] = op.Value
				// DPrintf(true, "applier %d update %v | %v", kv.me, op.Key, kv.db[op.Key])
			} else if op.Op == APPEND {
				kv.db[op.Key] = kv.db[op.Key] + op.Value
				// DPrintf(true, "applier %d update %v | %v", kv.me, op.Key, kv.db[op.Key])
			}
			// DPrintf(true, "applier %d msg %v", kv.me, msg)

			kv.lastRequest[op.ClientId] = op.RequestId

			if _, ok := kv.notify[msg.CommandIndex]; ok {
				// DPrintf(true, "applier notify %v me %v", kv.me, op.RequestId)
				kv.notify[msg.CommandIndex] <- true
			}

			if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate*8 {
				kv.dosnapshot(msg.CommandIndex)
			}
			kv.mu.Unlock()

		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				kv.setSnapshot(msg.Snapshot)
				kv.lastSnapshot = msg.SnapshotIndex
			}
			kv.mu.Unlock()
		}
	}
}

// func (kv *KVServer) snapshot() {
// 	for !kv.killed() {
// 		kv.mu.Lock()

// 		kv.mu.Unlock()
// 		time.Sleep(10 * time.Second)
// 	}
// }

// 生成快照,并传达给raft
func (kv *KVServer) dosnapshot(CommandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.db) != nil || e.Encode(kv.lastRequest) != nil {
		// DPrintf(true, "dosnapshot %d encode error", kv.me)
	} else {
		kv.lastSnapshot = CommandIndex
		kv.rf.Snapshot(CommandIndex, w.Bytes())
	}
}

func (kv *KVServer) setSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastRequest map[int64]int64
	if d.Decode(&db) != nil || d.Decode(&lastRequest) != nil {
		// DPrintf(true, "setSnapshot %d decode error me", kv.me)
	} else {
		kv.db = db
		kv.lastRequest = lastRequest
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// DPrintf(true, "StartKVServer %d", me)
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.notify = make(map[int]chan bool)
	kv.db = make(map[string]string)
	kv.lastRequest = make(map[int64]int64)
	kv.lastSnapshot = 0

	kv.setSnapshot(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.applier()

	// go kv.snapshot()

	time.Sleep(1 * time.Second)

	return kv
}
