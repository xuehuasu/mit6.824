package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// // for sorting by key.
// type ByKey []KeyValue

// // for sorting by key.
// func (a ByKey) Len() int           { return len(a) }
// func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
// func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type Maphost struct {
	addr string
	port int
}

func (c *Maphost) GetfileContent(filename string) []byte {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
}

func (c *Maphost) getFreePort() (string, error) {
	l, err := net.Listen("tcp", ":0") // 传入端口号 0 表示让系统自动分配一个可用端口
	if err != nil {
		return "", err
	}
	defer l.Close()

	addr := l.Addr().(*net.TCPAddr)
	c.port = addr.Port
	c.addr = addr.IP.String()
	return fmt.Sprintf("%d", addr.Port), nil
}

func (c *Maphost) listen() {
	rpc.Register(c)
	rpc.HandleHTTP()
	port, err := c.getFreePort()
	if err != nil {
		log.Fatal("can't get port")
	}
	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Maphost) getport() int {
	return c.port
}

func (c *Maphost) getaddr() string {
	return c.addr
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 启动监听，接受reduce的调用
	c := Maphost{}
	c.listen()

	{
		// Your worker implementation here.
		args := MapArgs{}
		reply := MapReply{}

		args.tasktype = "map"
		args.addr = c.getaddr()
		args.port = c.getport()

		for { // 循环获取map任务
			flag := call("Coordinator.getMapTask", &args, &reply)
			if flag == false {
				fmt.Fprint(os.Stderr, "get maptask error")
				return
			}
			if reply.isNull == false {
				// mapf(reply.inputfiles, )
				file, err := os.Open(reply.inputfiles)
				if err != nil {
					log.Fatalf("cannot open %v", reply.inputfiles)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", reply.inputfiles)
				}
				file.Close()
				kva := mapf(reply.inputfiles, string(content)) // KeyValue
				// 写入中间文件

				for i := 0; i < len(kva); i++ {
					reducen := ihash(reply.inputfiles) % reply.nReduce
					oname := "mr-tmp/mr-" + strconv.Itoa(args.port) + "-" + strconv.Itoa(reducen) // mr-tmp/mr-[map主机id]-[reduceId]

					args.outputfile[reducen] = true                                           // 记录输出文件
					ofile, _ := os.OpenFile(oname, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644) // 追加写

					enc := json.NewEncoder(ofile) // 编码器绑定文件
					enc.Encode(&kva[i])           // 编码并且写入
				}

				// 通知协调者完成任务
				args.taskid = reply.taskid
				call("Coordinator.finishTask", &args, &reply)
			} else {
				break
			}
		}
	}

	{

		args := ReduceArgs{}
		reply := ReduceReply{}

		args.addr = c.getaddr()
		args.port = c.getport()

		args.tasktype = "reduce"

		// 循环获取reduce任务
		for {
			flag := call("Coordinator.GetReduceTask", &args, &reply)
			if flag == false {
				fmt.Fprint(os.Stderr, "get maptask error")
				return
			}

			if reply.isNull == false {
				// 读取中间文件
				for i := 0; i < len(reply.inputfiles); i++ {
					maplocation := reply.inputfiles[i] // 目标主机
					mapaddr := maplocation[0 : len(maplocation)-5]
					mapport, _ := strconv.Atoi(maplocation[len(maplocation)-4:])

					callMap(mapaddr+":"+strconv.Itoa(mapport), "Maphost.GetfileContent", &args, &reply)
				}
			} else {
				break
			}
		}

		// 	file, err := os.Open(reply.inputfiles)
		// 	if err != nil {
		// 		log.Fatalf("cannot open %v", reply.inputfiles)
		// 	}
		// 	content, err := ioutil.ReadAll(file)
		// 	if err != nil {
		// 		log.Fatalf("cannot read %v", reply.inputfiles)
		// 	}
		// 	file.Close()

		// 	iname := reply.inputfiles
		// 	ifile, _ = os.Create(iname)

		// 	decoder := json.NewDecoder(ifile) // 解码器绑定文件
		// 	var keyValues []KeyValue
		// 	err = decoder.Decode(&keyValues) // 解码并且写入对象
		// 	if err != nil {
		// 		fmt.Println("JSON decoding error:", err)
		// 		return
		// 	}
		// 	ifile.Close()

		// 	sort.Sort(ByKey(keyValues))

		// 	oname := reply.outputfile + ".tmp"
		// 	ofile, _ := os.Create(oname)
		// 	enc := json.NewEncoder(ofile) // 编码器绑定文件

		// 	for i := 0; i < len(keyValues); {
		// 		j := i + 1
		// 		for j < len(keyValues) && keyValues[j].Key == keyValues[i].Key {
		// 			j++
		// 		}
		// 		values := []string{}
		// 		for k := i; k < j; k++ {
		// 			values = append(values, keyValues[k].Value)
		// 		}
		// 		output := reducef(keyValues[i].Key, values)

		// 		i = j
		// 	}

		// 	call("Coordinator.finishTask", &args, &reply)

	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
func callMap(rpctarget string, rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
