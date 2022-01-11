package mr

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	//存储map任务id->*Task
	mapTasks map[int]*Task
	//存储reduce任务id->*Task
	reduceTasks map[int]*Task
	//map任务通道
	mapChan chan *Task
	//reduce任务通道
	reduceChan chan *Task
	//当前是否已经处理完所有map任务
	mapFinished bool
	//互斥锁
	mu sync.Mutex
	//中间文件reduceId->Filenames,format : mr-X-Y (X表示map的id,Y表示reduce的id)
	interFiles map[int][]string
}

//任务结构体
type Task struct {
	//任务id,若为map类型则为0~len([]file)-1,若为reduce类型则为分区id,也就是0-NReduce-1
	Id int
	//任务类型: map/reduce
	TaskType string
	//map任务使用,源文件名
	Filename string
	//任务状态
	Status string
	//reduce任务使用,关于该reduce-id的全部文件
	Filenames []string
	//NReduce,map任务用于产生中间文件
	NReduce int
}

//是否已完成
func (s *Task) isFinished() bool {
	return s.Status == "finished"
}

//是否正在处理
func (s *Task) isWorking() bool {
	return s.Status == "working"
}

//是否空闲(未被处理)
func (s *Task) isFree() bool {
	return s.Status == "free"
}

//修改状态为working
func (s *Task) working() {
	s.Status = "working"
}

//修改状态为finished
func (s *Task) finished() {
	s.Status = "finished"
}

//修改状态为free
func (s *Task) free() {
	s.Status = "free"
}

// Your code here -- RPC handlers for the worker to call.

//分配Task并获取
func (c *Coordinator) GetTask(args *Args, reply *Reply) error {
	//访问和修改共享变量,需要加锁
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Println("[coordinator]: 接受任务请求")
	//当前还有map任务时
	if !c.mapFinished {
		//防止没有map的时候一直阻塞导致锁无法释放,先判断chan是否还有任务
		if len(c.mapChan) != 0 {
			//还有map任务则取出来分配
			task := <-c.mapChan
			//修改任务状态为working
			task.working()
			//放置返回值中
			reply.Task = *task
			log.Println("[coordinator]: 分配map任务:", task.Id)
			//协程检查10秒后是否完成
			go c.CheckFinished(task.Id, task.TaskType)
		}
		return nil
	}
	//当前所有map任务已被完成
	//防止没有map的时候一直阻塞导致锁无法释放,先判断chan是否还有任务
	if len(c.reduceChan) != 0 {
		//还有reduce任务则取出来分配
		task := <-c.reduceChan
		//修改任务状态为working
		task.working()
		//将中间文件放入到task中
		task.Filenames = c.interFiles[task.Id]
		//放置返回值中
		reply.Task = *task
		log.Println("[coordinator]: 分配reduce任务:", task.Id)
		//协程检查10秒后是否完成
		go c.CheckFinished(task.Id, task.TaskType)
	}
	return nil
}

//通知已经完成task
func (c *Coordinator) FinishedTask(args *Args, reply *Reply) error {
	//处理共享变量,加锁
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Println("[coordinator]: 接受完成任务请求")
	//获取任务
	taskArg := args.Task
	var task *Task
	//根据任务id和任务类型获取到本地存的任务
	if taskArg.TaskType == "map" {
		task = c.mapTasks[taskArg.Id]
	} else {
		task = c.reduceTasks[taskArg.Id]
	}
	log.Println("[coordinator]: 接受到已完成的", task.TaskType, "类型任务", task.Id)
	//判断状态是否是working(如果有一个任务由于卡顿导致coordinator重新加入任务到chan中了,那么会出现这种此时刚好完成通知了,防止重复提交
	//只取task状态为working的时候提交过来的,其他情况不处理
	if task.isWorking() {
		//更新状态为finished
		task.finished()
		//如果是map任务被完成,则需要创建一个reduce任务,则需要取出传来的中间文件名
		if task.TaskType == "map" {
			filenames := args.Filenames
			//将文件按照reduce-id分别加到不同的切片中
			for _, filename := range filenames {
				split := strings.Split(filename, "-")
				reduceId, _ := strconv.Atoi(split[2])
				c.interFiles[reduceId] = append(c.interFiles[reduceId], filename)
			}
			log.Println("[coordinator]: 完成map任务:", task.Id)
			return nil
		}
		//如果是reduce任务被完成,没有额外处理
		log.Println("[coordinator]: 完成reduce任务:", task.Id, ",输出到文件", args.Filename)
	}
	log.Println("[coordinator]: 重复的任务完成请求,不做处理,任务:", task.Id)
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	go func() {
		for {
			log.Println("go routine running...")
			time.Sleep(1000)
		}
	}()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	//注册rpc服务
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("[coordinator]: 启动http.Serve")
	go http.Serve(l, nil)
}

//作为协程启动检查该Task在十秒后是否已经完成
func (c *Coordinator) CheckFinished(taskId int, taskType string) {

	log.Println("[coordinator]: 协程检查10s后", taskType, "任务:", taskId, "是否完成")
	//停止10s
	time.Sleep(10 * time.Second)
	//加锁
	c.mu.Lock()
	defer c.mu.Unlock()
	//判断task类型
	if taskType == "map" {
		t := c.mapTasks[taskId]
		if t.isFinished() {
			//若已经完成,则结束
			log.Println("[coordinator]: 10s后", taskType, "任务:", taskId, "已完成")
			return
		}
		//未完成,则将状态改为free,并且重新加入到chan中
		t.free()
		c.mapChan <- t
		log.Println("[coordinator]: 10s后", t.TaskType, "任务:", t.Id, "未完成,重新加入处理该任务")
	} else {
		t := c.reduceTasks[taskId]
		if t.isFinished() {
			//若已经完成,则结束
			log.Println("[coordinator]: 10s后", t.TaskType, "任务:", t.Id, "已完成")
			return
		}
		//未完成,则将状态改为free,并且重新加入到chan中
		t.free()
		c.reduceChan <- t
		log.Println("[coordinator]: 10s后", taskType, "任务:", taskId, "未完成,重新加入处理该任务")
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//操作共享变量加锁
	c.mu.Lock()
	defer c.mu.Unlock()
	mapAllFinished := true
	reduceAllFinished := true
	// Your code here.
	//遍历所有任务,判断是否都为finished
	//先判断map任务,而且当所有map都finished的时候,更新coordinator的mapFinished状态
	for _, task := range c.mapTasks {
		if !task.isFinished() {
			mapAllFinished = false
		}
	}
	//更新
	c.mapFinished = mapAllFinished
	//若此时mapAllFinished已经为false,那么直接返回false
	if !mapAllFinished {
		return false
	}
	log.Println("[coordinator]: 目前map任务是否全部完成:", mapAllFinished)
	//遍历所有reduce任务
	for _, task := range c.reduceTasks {
		if !task.isFinished() {
			//log.Println("[coordinator]: reduce任务", task.Id, "状态为", task.Status)
			reduceAllFinished = false
		}
	}
	log.Println("[coordinator]: 目前所有任务是否全部完成:", reduceAllFinished)
	return reduceAllFinished
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//实例化一个coordinator
	c := Coordinator{
		mapTasks:    make(map[int]*Task),
		reduceTasks: make(map[int]*Task),
		mapChan:     make(chan *Task, 10000),
		reduceChan:  make(chan *Task, nReduce),
		mapFinished: false,
		interFiles:  make(map[int][]string),
	}
	for _, file := range files {
		log.Println("[coordinator]: 传入文件", file)
	}
	// Your code here.
	//初始化map任务
	for i, file := range files {
		//实例化task
		t := &Task{
			Id:       i,
			TaskType: "map",
			Filename: file,
			Status:   "free",
			NReduce:  nReduce,
		}
		//加入tasks
		c.mapTasks[i] = t
		//加入mapChan
		c.mapChan <- t
	}
	log.Println("[coordinator]: 完成map任务实例化")
	//初始化reduce任务
	for i := 0; i < nReduce; i++ {
		//实例化task
		t := &Task{
			Id:        i,
			TaskType:  "reduce",
			Status:    "free",
			Filenames: make([]string, 0),
			NReduce:   nReduce,
		}
		//加入reduceTask
		c.reduceTasks[i] = t
		//加入reduceChan
		c.reduceChan <- t
	}
	log.Println("[coordinator]: 完成reduce任务初始化")
	c.server()
	return &c
}
