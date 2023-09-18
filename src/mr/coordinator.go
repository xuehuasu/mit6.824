package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	idx     int
	nreduce int

	filename map[int]string

	// 已分配的任务为 true，未分配的任务为 false
	mapTask    map[int]bool
	reduceTask map[int]bool

	// 已完成的任务为 true，未完成的任务为 false
	achieveMapTask    map[int]bool
	achieveReduceTask map[int]bool

	// 用于判断是否完成任务
	achievemap    chan bool
	achievereduce chan bool

	// 互斥锁
	mutex sync.Mutex
	// 条件变量
	cond *sync.Cond

	// 中间文件id对map主机地址的映射 int -> []string , 文件id是根据nreduce分配的
	midfile map[int][]string
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// <-achieve

	return ret
}

func (c *Coordinator) GetMapTask(arg *MapArgs, reply *MapReply) error {
	reply.isNull = false
	reply.nReduce = c.nreduce

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i, filename := range c.filename {
		if c.mapTask[i] == false { // 未分配的任务
			reply.inputfiles = filename
			reply.taskid = i
			c.mapTask[i] = true
			return nil
		}
	}

	for i, filename := range c.filename {
		if c.achieveMapTask[i] == false { // 未完成的任务
			reply.inputfiles = filename
			reply.taskid = i
			return nil
		}
	}
	reply.isNull = true
	return nil
}

func (c *Coordinator) GetReduceTask(arg *ReduceArgs, reply *ReduceReply) error {
	reply.isNull = false
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for i := 0; i <= c.nreduce; i++ {
		if c.reduceTask[i] == false { // 未分配的任务
			reply.inputfiles = "mr-mid-" + strconv.Itoa(i)
			reply.taskid = i
			reply.outputfile = "mr-reduce-" + strconv.Itoa(i)
			c.reduceTask[i] = true
			return nil
		}
	}

	for i, _ := range c.filename {
		if c.achieveReduceTask[i] == false { // 未完成的任务
			reply.inputfiles = "mr-mid-" + strconv.Itoa(i)
			reply.taskid = i
			reply.outputfile = "mr-reduce-" + strconv.Itoa(i)
			return nil
		}
	}
	reply.isNull = true
	return nil
}

func (c *Coordinator) FinishTask(arg *MapArgs, reply *MapReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if arg.tasktype == "map" {
		if c.achieveMapTask[arg.taskid] == false { // 仅记录第一次成功的任务，重复的任务不记录
			c.achieveMapTask[arg.taskid] = true
			for fileid, flag := range arg.outputfile { // 记录中间文件对map主机地址的映射
				if flag == true {
					c.midfile[fileid] = append(c.midfile[fileid], arg.addr+":"+strconv.Itoa(arg.port))
				}
			}
		}

		return nil
	} else if arg.tasktype == "reduce" {
		if c.achieveReduceTask[arg.taskid] == true {
			return nil
		}
		c.achieveReduceTask[arg.taskid] = true
		olefilname := arg.outputfile
		newfilname := "mr-out-" + strconv.Itoa(arg.taskid)
		os.Rename(olefilname, newfilname)
		return nil
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.

	c.idx = 0
	c.nreduce = nReduce

	c.filename = make(map[int]string)
	c.mapTask = make(map[int]bool)
	c.reduceTask = make(map[int]bool)
	c.achieveMapTask = make(map[int]bool)
	c.achieveReduceTask = make(map[int]bool)

	c.achievemap = make(chan bool)
	c.achievereduce = make(chan bool)

	c.cond = sync.NewCond(&c.mutex)

	c.midfile = make(map[int][]string)

	for i, filename := range files {
		c.filename[i] = filename
	}

	c.server()
	return &c
}
