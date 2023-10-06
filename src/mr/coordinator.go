package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"6.824/timer"
)

type Coordinator struct {
	// Your definitions here.
	idx     int
	nreduce int

	filename map[int]string

	// 已分配的任务为true，未分配的任务为false
	mapTask    map[int]bool
	reduceTask map[int]bool

	// 已完成的任务为 结果地址，未完成的任务为 ""
	// taskid -> fileaddr
	achieveMapTask    map[int]string
	achieveReduceTask map[int]bool

	// 用于判断是否完成任务
	achievemap    chan bool
	achievereduce chan bool

	// 互斥锁
	mutex sync.Mutex
	// 条件变量
	cond *sync.Cond

	// 中间文件id对文件地址的映射 int -> []string , 文件id是根据nreduce分配的
	midfile map[int][]string

	// 完成map
	achievmap     bool
	achievmapchan chan bool
	// 完成所有
	achieveall     bool
	achieveallchan chan bool

	// 为每一个Task设置超时器
	maptimer    map[int]*timer.Timer
	reducetimer map[int]*timer.Timer
	timeout     time.Duration
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	// fmt.Print("server start\n")
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.

func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	for ret == false {
		ret = <-c.achieveallchan
	}
	c.cond.Broadcast()
	// fmt.Print("任务完成", "\n")

	return true
}

func (c *Coordinator) GetMapTask(arg *MapArgs, reply *MapReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// fmt.Print(arg.Port, " get map task", "\n")
	if c.achievmap {
		reply.Timeout = c.timeout + c.timeout
		// fmt.Print(arg.Port, " set map timer", reply.Timeout, "\n")
		c.cond.Broadcast()
		return nil
	}
	achieve := true
	for i, filename := range c.filename {
		if c.achieveMapTask[i] == "" {
			achieve = false
			// 未完成的任务有可能是第一次分配，也有可能是map主机失效后重新分配
			if c.mapTask[i] == false {
				// fmt.Print(arg.Port, " 分配map任务: ", i, " ", filename, "\n")
				c.mapTask[i] = true
				reply.Inputfiles = filename
				reply.Taskid = i
				reply.NReduce = c.nreduce
				c.maptimer[i] = timer.NewTimer(c.timeout, func() {
					c.mutex.Lock()
					defer c.mutex.Unlock()
					c.mapTask[i] = false // 设置为false，表示任务重新分配
					if c.achieveMapTask[i] == "" {
						// fmt.Print("mapTask超时: ", i, "\n")
					}
				})
				return nil
			}
		}
	}

	c.achievmap = achieve

	if achieve == false {
		// 告诉map，多久之后再来获取任务
		reply.Timeout = c.timeout / 2
	} else {
		reply.Timeout = c.timeout + c.timeout
		c.cond.Broadcast()
	}
	// fmt.Print(arg.Port, " set map timer", reply.Timeout, "\n")
	return nil
}

func (c *Coordinator) GetReduceTask(arg *ReduceArgs, reply *ReduceReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for c.achievmap == false {
		c.cond.Wait()
	}
	// fmt.Print(arg.Port, " get reduce task", "\n")
	if c.achieveall {
		reply.Timeout = c.timeout
		return nil
	}
	achieve := true
	for i := 1; i <= c.nreduce; i++ {
		if c.achieveReduceTask[i] == false { // 未完成的任务
			achieve = false
			if c.reduceTask[i] == false { // 未分配的任务
				c.reduceTask[i] = true
				reply.Taskid = i
				for j, mapaddr := range c.achieveMapTask {
					if mapaddr != "" {
						filename := "mr-" + mapaddr[strings.Index(mapaddr, ":")+1:] + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
						reply.Inputfiles = append(reply.Inputfiles, mapaddr+" "+filename)
					}

				}
				// fmt.Print(arg.Port, " 分配reduce任务: ", i, "\n")
				c.reducetimer[i] = timer.NewTimer(c.timeout, func() {
					c.mutex.Lock()
					defer c.mutex.Unlock()
					c.reduceTask[i] = false // 设置为false，表示任务重新分配
					if c.achieveReduceTask[i] == false {
						// fmt.Print("reduceTask超时: ", i, "\n")
					}
				})
				return nil
			}
		}
	}

	if achieve {
		c.achieveall = achieve
		defer func() {
			c.achieveallchan <- true
		}()
	}
	reply.Timeout = c.timeout / 2
	return nil
}

func (c *Coordinator) MapFailure(arg *ReduceArgs, reply *string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// addr:port mr-[map主机id]-[reduceId]-[任务id] 提取任务名
	fillocal := arg.Outputfile[0]
	ids := strings.SplitN(fillocal, "-", 4)
	taskid, _ := strconv.Atoi(ids[3])

	// mapaddr := arg.Addr + strconv.Itoa(arg.Port)
	// fillocal[0:strings.Index(fillocal, " ")]
	// fmt.Print("map失效Taskid: ", taskid, " 文件名: ", c.filename[taskid], " reduceId: ", arg.Taskid, " mapaddr", c.achieveMapTask[taskid], "\n")
	mapaddr := arg.Outputfile[0][0:strings.Index(arg.Outputfile[0], " ")]

	if mapaddr == c.achieveMapTask[taskid] { // 不等于的话说明已经更新过map地址
		// 阻塞reduce分配任务
		c.achievmap = false
		// 重新分配map主机上的任务
		for i, _ := range c.filename {
			if c.achieveMapTask[i] == mapaddr { // 所有在失效map上运行过的任务，都要重新分配
				c.mapTask[i] = false
				c.achieveMapTask[i] = ""
			}
		}
		c.achievmap = false        // 阻塞reduce分配任务
		for c.achievmap == false { // 等待map完成
			c.cond.Wait()
		}
	}

	// addr:port mr-[map主机id]-[reduceId]-[任务id]
	mapaddr = c.achieveMapTask[taskid]
	filename := "mr-" + mapaddr[strings.Index(mapaddr, ":")+1:] + "-" + strconv.Itoa(arg.Taskid) + "-" + strconv.Itoa(taskid)
	*reply = mapaddr + " " + filename // 将可用的文件地址传回去
	return nil
}

func (c *Coordinator) FinishMapTask(arg *MapArgs, reply *MapReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// fmt.Print(arg.Port, " map完成 ", arg.Taskid, "\n")
	if c.achieveMapTask[arg.Taskid] == "" { // 仅记录第一次成功的任务，重复的任务不记录
		c.achieveMapTask[arg.Taskid] = arg.Addr + ":" + strconv.Itoa(arg.Port)
		for fileid, _ := range arg.Outputfile { // 记录中间文件对文件地址的映射
			fileaddr := arg.Addr + ":" + strconv.Itoa(arg.Port) + " " + arg.Outputfile[fileid]
			c.midfile[fileid] = append(c.midfile[fileid], fileaddr)
			// fmt.Print("map中间文件 ", fileid, " ", fileaddr, "\n")
		}
	}

	return nil
}

func (c *Coordinator) FinishReduceTask(arg *ReduceArgs, reply *ReduceReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// fmt.Print(arg.Port, "reduce完成 ", arg.Taskid, "\n")
	if !c.achieveReduceTask[arg.Taskid] {
		c.achieveReduceTask[arg.Taskid] = true
		filename := arg.Outputfile[0]
		// 修改文件名，并将其移动到mr-out-id
		os.Rename(filename, "mr-out-"+strconv.Itoa(arg.Taskid))
	}

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.achieveall = false
	c.achieveallchan = make(chan bool)
	c.achievmap = false
	c.achievmapchan = make(chan bool)

	c.idx = 0
	c.nreduce = nReduce

	c.filename = make(map[int]string)
	c.mapTask = make(map[int]bool)
	c.reduceTask = make(map[int]bool)
	c.achieveMapTask = make(map[int]string)
	c.achieveReduceTask = make(map[int]bool)

	c.achievemap = make(chan bool)
	c.achievereduce = make(chan bool)

	c.cond = sync.NewCond(&c.mutex)

	c.midfile = make(map[int][]string)

	c.maptimer = make(map[int]*timer.Timer)
	c.reducetimer = make(map[int]*timer.Timer)
	c.timeout = 6 * time.Second // 默认超时时间

	for i, filename := range files {
		c.filename[i+1] = filename
	}
	c.server()
	return &c
}
