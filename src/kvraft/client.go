package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu        sync.Mutex
	leaderId  int
	clientId  int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.requestId = 0
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	ck.requestId += 1
	// requestId := ck.requestId
	// clientId := ck.clientId
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	ck.mu.Unlock()

	for {
		ck.mu.Lock()
		leaderid := ck.leaderId
		ck.mu.Unlock()
		doneCh := make(chan bool, 1)
		reply := GetReply{}
		go func() {
			ok := ck.servers[leaderid].Call("KVServer.Get", &args, &reply)
			doneCh <- ok
		}()

		select {
		case <-time.After(5 * time.Second):
			// DPrintf(true, "client Get %d timeout %d %d", clientId, requestId, leaderid)
		case ok := <-doneCh:
			if ok && reply.Err == OK {
				return reply.Value
			} else if !ok {
				// DPrintf(true, "client Get %d rpc error %d %d", clientId, requestId, leaderid)
			}
		}
		time.Sleep(300 * time.Millisecond)
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		// DPrintf(true, "client Get %d change leader %d %d %d", clientId, requestId, leaderid, ck.leaderId)
		ck.mu.Unlock()
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	ck.requestId += 1
	// requestId := ck.requestId
	// clientId := ck.clientId
	args := PutAppendArgs{
		Op:        op,
		Key:       key,
		Value:     value,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	ck.mu.Unlock()

	for {
		ck.mu.Lock()
		leaderid := ck.leaderId
		ck.mu.Unlock()

		doneCh := make(chan bool, 1)
		reply := PutAppendReply{}
		go func() {
			ok := ck.servers[leaderid].Call("KVServer.PutAppend", &args, &reply)
			doneCh <- ok
		}()

		select {
		case <-time.After(5 * time.Second):
			// DPrintf(true, "client PutAppend %d timeout %d %d", clientId, requestId, leaderid)
		case ok := <-doneCh:
			if ok && reply.Err == OK {
				return
			} else if !ok {
				// DPrintf(true, "client PutAppend %d rpc error %d %d", clientId, requestId, leaderid)
			}
		}
		time.Sleep(300 * time.Millisecond)
		ck.mu.Lock()
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
		// DPrintf(true, "client PutAppend %d change leader %d %d %d", clientId, requestId, leaderid, ck.leaderId)
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
