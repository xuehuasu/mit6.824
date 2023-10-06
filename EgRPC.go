package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"
)

//
// Common RPC request/reply definitions
//

type PutArgs struct {
	Key   int
	Value int
}

type PutReply struct {
	Key   int
	Value int
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

//
// Client
//

func get(key string) string {
	client, err := rpc.Dial("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	args := GetArgs{}
	reply := GetReply{}
	err = client.Call("KV.Get", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	client.Close()
	return reply.Value
}

func put(key int, val int) {
	client, err := rpc.Dial("tcp", "127.0.0.1:1234")
	if err != nil {
		log.Fatal("dialing:", err)
	}

	args := PutArgs{key, val}
	reply := PutReply{}
	err = client.Call("KV.Put", &args, &reply)
	if err != nil {
		log.Fatal("error:", err)
	}
	fmt.Print(reply.Value)
	client.Close()
}

//
// Server
//

type KV struct {
	mu sync.Mutex
	// data map[string]string
	data int
}

func server() *KV {
	// kv := &KV{data: map[string]string{}}
	kv := &KV{}
	rpcs := rpc.NewServer()
	rpcs.Register(kv)
	l, e := net.Listen("tcp", ":1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()
	return kv
}

func (kv *KV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// reply.Value = kv.data[args.Key]

	return nil
}

func (kv *KV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// kv.data[args.Key] = args.Value
	kv.data = args.Value
	reply.Value = args.Value
	reply.Key = reply.Key

	return nil
}

//
// main
//

func main() {
	server()
	// time.Sleep(100 * time.Second)
	// put("subject", "6.5840")
	// fmt.Printf("Put(subject, 6.5840) done\n")
	// fmt.Printf("get(subject) -> %s\n", get("subject"))
	for i := 0; i < 100; i++ {
		put(i, i+1)
		time.Sleep(2 * time.Second)
	}
}
