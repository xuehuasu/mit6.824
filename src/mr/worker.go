package mr

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"6.824/labgob"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Maphost struct {
	Addr string
	Port int
}

func (c *Maphost) GetfileContent(filename string, reply *[]byte) error {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("获取文件信息失败")
		return err
	}

	fileSize := fileInfo.Size()
	buffer := make([]byte, fileSize)

	_, err = file.Read(buffer)
	if err != nil {
		fmt.Println("读取文件失败")
		return err
	}

	*reply = buffer
	return nil
}

func GetFreePort(addr *string, port *int) error {
	l, err := net.Listen("tcp", ":0") // 传入端口号 0 表示让系统自动分配一个可用端口
	if err != nil {
		return err
	}
	defer l.Close()

	laddr := l.Addr().(*net.TCPAddr)
	*port = laddr.Port

	// 获取主机名
	*addr = "127.0.0.1"
	return nil
}

func (c *Maphost) Listen() {
	rpc := rpc.NewServer()
	rpc.Register(c)
	err := GetFreePort(&c.Addr, &c.Port)
	if err != nil {
		log.Fatal("can't get port")
	}
	l, e := net.Listen("tcp", c.Addr+":"+strconv.Itoa(c.Port))

	// fmt.Print("listen on ", c.Addr+":"+strconv.Itoa(c.Port), "\n")

	if e != nil {
		log.Fatal("listen error:", e)
		os.Exit(1)
	}
	go func() {
		for {
			conn, err := l.Accept()
			if err == nil {
				go rpc.ServeConn(conn)
			} else {
				break
			}
		}
		l.Close()
	}()
}

func (c *Maphost) Getport() int {
	return c.Port
}

func (c *Maphost) Getaddr() string {
	return c.Addr
}

var achievemapchan chan bool
var achievemap bool

var achievereducechan chan bool
var achievereduce bool

func (c *Maphost) Achievereduce(arg interface{}, reply interface{}) {
	if !achievereduce {
		achievereduce = true
		defer func() {
			achievereducechan <- true
		}()
	}
}

func mapworker(c *Maphost, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, wg *sync.WaitGroup) {
	wg.Add(1)
	// Your worker implementation here.
	args := MapArgs{}
	reply := MapReply{}

	args.Tasktype = "map"
	args.Addr = c.Getaddr()
	args.Port = c.Getport()

	for { // 循环获取map任务
		reply.Inputfiles = ""
		reply.Taskid = -1
		err := call("Coordinator.GetMapTask", &args, &reply)

		if err != nil {
			// fmt.Fprintln(os.Stderr, " call maptask error\n")
			os.Exit(1)
		}
		args.Outputfile = make(map[int]string)
		if reply.Taskid != -1 {

			file, err := os.Open(reply.Inputfiles)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Inputfiles)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Inputfiles)
			}
			file.Close()
			kva := mapf(reply.Inputfiles, string(content)) // []KeyValue

			// 创建NReduce个buffer
			buffers := make([][]KeyValue, reply.NReduce)
			for i := 0; i < len(kva); i++ {
				reducen := ihash(kva[i].Key) % reply.NReduce
				buffers[reducen] = append(buffers[reducen], kva[i])
			}

			// 写入中间文件
			// 命名规范：mr-[map主机id]-[reduce文件Id]
			intermediatename := "mr-" + strconv.Itoa(args.Port) // mr-[map主机id]

			for i := 1; i <= reply.NReduce; i++ {
				oname := intermediatename + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.Taskid) // mr-[map主机id]-[reduceId]-[任务id]
				ofile, _ := os.Create(oname)
				ens := labgob.NewEncoder(ofile)

				for _, kv := range buffers[i-1] {
					err := ens.Encode(&kv)
					if err != nil {
						log.Fatalf("cannot encoding %v", kv)
					}
				}
				ofile.Close()
			}

			// 通知协调者完成任务
			args.Taskid = reply.Taskid
			call("Coordinator.FinishMapTask", &args, &reply)

		} else {
			// fmt.Print(args.Port, " map sleep : ", reply.Timeout, "\n")
			if reply.Timeout == 0 {
				reply.Timeout = 1 * time.Second
			}
			time.Sleep(reply.Timeout) // 等待一段时间，再次获取任务

		}
	}
}

func reducework(c *Maphost, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, wg *sync.WaitGroup) {
	wg.Add(1)
	args := ReduceArgs{}
	reply := ReduceReply{}

	reply.Inputfiles = make([]string, 0)

	args.Addr = c.Getaddr()
	args.Port = c.Getport()

	args.Tasktype = "reduce"
	args.Outputfile = make(map[int]string)

	// 循环获取reduce任务
	for {
		reply.Taskid = -1
		err := call("Coordinator.GetReduceTask", &args, &reply)
		if err != nil {
			// fmt.Print(args.Port, " call reducetask error\n")
			os.Exit(1)
		}
		// fmt.Print("reduce task: ", reply.Taskid, "\n")

		if reply.Taskid != -1 {
			// // 读取中间文件

			args.Taskid = reply.Taskid
			intermediate := []KeyValue{}

			// fmt.Print(args.Port, " reply.Inputfiles: ", reply.Inputfiles, "\n")
			// fmt.Print(args.Port, " len(reply.Inputfiles): ", len(reply.Inputfiles), "\n")

			for i := 0; i < len(reply.Inputfiles); i++ {
				// addr:port filename
				fillocal := reply.Inputfiles[i]

				// port := maplocal[strings.Index(maplocal, ":")+1 : strings.Index(maplocal, " ")]

				maplocal := fillocal[0:strings.Index(fillocal, " ")]
				filename := fillocal[(strings.Index(fillocal, " ") + 1):]

				var filereply []byte
				err := callMap("Maphost.GetfileContent", maplocal, filename, &filereply)
				// fmt.Print("maplocal", maplocal, " filename: ", filename, "\n")
				if err != nil {
					// fmt.Print(args.Port, " get file error id :", reply.Taskid, " error: ", err, "\n")
					// 向master报告任务失败
					var newfile string
					args.Outputfile[0] = fillocal
					err := call("Coordinator.MapFailure", &args, &newfile) // 不出意外的话可以获取到新文件
					if err != nil {
						// fmt.Print("Coordinator.MapFailure error\n")
						os.Exit(1)
					}
					// fmt.Print("old file: ", reply.Inputfiles[i], "\n")
					// fmt.Print("new file: ", newfile, "\n")

					reply.Inputfiles[i] = newfile
					i--
					continue
				}
				// fmt.Print(args.Port, "reply task: ", reply.Taskid, " inputfile ", filename, "\n")
				// 解码
				buffer := bytes.NewBuffer(filereply)
				d := labgob.NewDecoder(buffer)
				for {
					var kv KeyValue
					err := d.Decode(&kv)
					if err == io.EOF {
						// 已达到数据流的末尾，退出循环
						break
					} else if err != nil {
						log.Fatalf("cannot decode data: %v", err)
						return
					}
					// fmt.Print(kv, "\n") // {k, v}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate)) // 排序
			oname := "mr-tmp-out-" + strconv.Itoa(args.Port) + "-" + strconv.Itoa(reply.Taskid)
			ofile, _ := os.Create(oname)

			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()
			args.Outputfile[0] = oname
			args.Taskid = reply.Taskid
			call("Coordinator.FinishReduceTask", &args, &reply)
			// fmt.Print("intermediate len: "+strconv.Itoa(len(intermediate)), "\n")
		} else {
			// fmt.Print(args.Port, " reduce sleep : ", reply.Timeout, "\n")
			if reply.Timeout == 0 {
				reply.Timeout = 1 * time.Second
			}
			time.Sleep(reply.Timeout) // 等待一段时间，再次获取任务
		}
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 启动监听，接受reduce的调用
	c := Maphost{}
	c.Listen()

	gob.RegisterName("KeyValue", KeyValue{})
	achievemapchan = make(chan bool)
	achievemap = false
	achievereducechan = make(chan bool)
	achievereduce = false

	var wg sync.WaitGroup

	// 设置等待的线程数量
	wg.Add(2)

	go mapworker(&c, mapf, reducef, &wg)

	go reducework(&c, mapf, reducef, &wg)

	// 等待所有线程退出
	wg.Wait()
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// var mutex sync.Mutex

func call(rpcname string, args interface{}, reply interface{}) error {
	sockname := coordinatorSock() // sockname =  "/var/tmp/824-mr-1000"
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		// fmt.Print("dialing master1: ", err, "\n")
		return err
	}
	defer c.Close()
	err = c.Call(rpcname, args, reply)
	if err != nil {
		// fmt.Print("dialing master2: ", err, "\n")
		return err
	}
	return nil
}

func callMap(rpcfunname string, rpctarget string, file interface{}, reply interface{}) error {
	c, err := rpc.Dial("tcp", rpctarget)
	if err != nil {
		// fmt.Print("dialing map1: ", err, "\n")
		return err
	}
	defer c.Close()
	err = c.Call(rpcfunname, file, reply) //unexpected EOF
	if err != nil {
		// fmt.Print("dialing map2: ", err, "\n")
		return err
	}

	return nil
}
