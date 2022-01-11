package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	log.Println("[woker]: worker已被创建")
	// Your worker implementation here.
	//不断请求任务
	for {
		log.Println("[worker]: 发起rpc调用GetTask方法")
		task, ok := CallForGetTask()
		if !ok {
			//rpc请求异常,认为coordinator已结束,worker也结束
			log.Fatalf("[worker]: GetTask的rpc调用错误,worker结束")
			return
		}
		if task.TaskType == "" {
			log.Println("[worker]: 当前没有任务")
			//当task为空时,没有请求到任务,sleep1秒后再次请求
			time.Sleep(3 * time.Second)
			continue
		}
		//成功获取到task
		log.Println("[worker]: 获取到", task.TaskType, "类型任务:", task.Id)
		if task.TaskType == "map" {
			//处理map任务
			filenames := MapWork(task, mapf)
			task.Filenames = filenames
			log.Println("[worker]:", task.TaskType, "类型任务:", task.Id, "已完成")
			//通知coordinator
			ok := CallForFinishedTask(task)
			if !ok {
				log.Fatalf("[worker]: FinishedTask的rpc调用错误,worker结束")
				return
			}
		} else {
			//reduce任务
			filename := ReduceWork(task, reducef)
			log.Println("[worker]:", task.TaskType, "类型任务:", task.Id, "已完成")
			//通知coordinator
			task.Filename = filename
			ok := CallForFinishedTask(task)
			if !ok {
				log.Fatalf("[worker]: FinishedTask的rpc调用错误,worker结束")
				return
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

//处理map任务
func MapWork(task *Task, mapFunc func(string, string) []KeyValue) (filenames []string) {
	//需要处理的文件名
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	//获取该文件的key->val集合
	kva := mapFunc(filename, string(content))
	//创建存取每个文件的encoder
	encodersMap := make(map[int]*json.Encoder)
	for i := 0; i < task.NReduce; i++ {
		//创建format: mr-x-y ,x表示当前map任务id,y表示0~nReduce中的一个分区
		name := "mr-" + strconv.Itoa(task.Id) + "-" + strconv.Itoa(i)
		file, err := os.Create(name)
		//将文件名加入到产生的文件列表
		filenames = append(filenames, name)
		//log.Println("[worker]: 将文件", file.Name(), "加入到产生的文件列表中")
		if err != nil {
			log.Fatalf("create file: %s error: %s", file.Name(), err.Error())
		}
		encodersMap[i] = json.NewEncoder(file)
	}
	//将每个kv以json编码的格式写到当前的map任务id的nReduce个文件中
	for _, kv := range kva {
		//获取该key分区id
		i := ihash(kv.Key) % task.NReduce
		//写入对应文件中
		//获取编码器
		encoder := encodersMap[i]
		//json编码到文件中
		err = encoder.Encode(&kv)
		if err != nil {
			log.Fatalf("encode error: %s", err.Error())
		}
	}
	for _, filename := range filenames {
		log.Println("[worker]: 产生中间文件", filename)
	}
	return filenames
}

//处理reduce任务
func ReduceWork(task *Task, reduceFunc func(string, []string) string) (filename string) {
	//解析传来的目标中间文件
	filenames := task.Filenames
	for _, filename := range filenames {
		log.Println("[worker]: 需要提取文件", filename)
	}
	//加载到内存的中间文件中的键值对
	intermediates := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("open file: %s error: %s", filename, err.Error())
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediates = append(intermediates, kv)
		}

	}
	//创建临时文件
	tempFile, err := ioutil.TempFile("", "mr-temp*")
	if err != nil {
		log.Fatalf("crete temp file error: %s", err.Error())
	}
	//将内存中的键值对进行排序
	sort.Sort(ByKey(intermediates))
	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := reduceFunc(intermediates[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediates[i].Key, output)
		i = j
	}
	//将临时文件原子性命名
	filename = "mr-out-" + strconv.Itoa(task.Id)
	err = os.Rename(tempFile.Name(), filename)
	if err != nil {
		log.Fatalf("rename temp file error: %s", err.Error())
	}
	tempFile.Close()
	return filename
}
func CallForGetTask() (*Task, bool) {
	args := &Args{}
	reply := &Reply{}
	ok := call("Coordinator.GetTask", args, reply)
	return &reply.Task, ok
}

func CallForFinishedTask(task *Task) bool {
	args := &Args{
		Task: *task,
	}
	reply := &Reply{}
	ok := call("Coordinator.FinishedTask", args, reply)
	return ok
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	log.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
