package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// Add your RPC definitions here.

type MapArgs struct {
	addr       string // worker地址
	port       int    // worker端口
	taskid     int
	tasktype   string
	outputfile map[int]bool
}

type MapReply struct {
	isNull     bool
	taskid     int
	inputfiles string
	nReduce    int
}

type ReduceArgs MapArgs

type ReduceReply struct {
	isNull     bool
	taskid     int
	inputfiles []int
	outputfile string
	nReduce    int
	addr       string // map worker地址
	port       int    // map worker端口
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.

func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
