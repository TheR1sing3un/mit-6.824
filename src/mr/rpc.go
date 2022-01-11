package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Args struct {
	//可选参数为task任务(FinishedTask的RPC调用时的参数)
	Task Task
	//可选参数为filename(处理后输出的filename,map为中间文件,reduce为最终文件)
	Filenames []string
}

type Reply struct {
	//可选参数为Task任务
	Task Task
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
