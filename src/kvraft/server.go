package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type CommandType string

const (
	PutMethod    = "Put"
	AppendMethod = "Append"
	GetMethod    = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType CommandType //指令类型(put/append/get)
	Key         string      //键
	Value       string      //值(Get请求时此处为空)
	ClientId    int64       //client的唯一id
	CommandId   int         //命令的唯一id
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientReply map[int64]CommandContext    //客户端的唯一指令和响应的对应map
	kvData      map[string]string           //kv数据
	replyChMap  map[int]chan ApplyNotifyMsg //某index的响应的chan
}

// ApplyNotifyMsg 可表示GetReply和PutAppendReply
type ApplyNotifyMsg struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
	//该被应用的command的term,便于RPC handler判断是否为过期请求(之前为leader并且start了,但是后来没有成功commit就变成了follower,导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
	//或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader的commit的log了
	Term int
}

type CommandContext struct {
	commandId int            //该client的目前的commandId
	reply     ApplyNotifyMsg //该command的响应
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	//defer kv.mu.Unlock()
	defer func() {
		log.Printf("kvserver[%d]: 返回Get RPC请求,args=[%v];reply=[%v]\n", kv.me, args, reply)
	}()
	log.Printf("kvserver[%d]: 接收Get RPC请求,args=[%v]\n", kv.me, args)
	//1.先判断该命令是否已经被执行过了
	if commandContext, ok := kv.clientReply[args.ClientId]; ok {
		if commandContext.commandId >= args.CommandId {
			//若当前的请求已经被执行过了,那么直接返回结果
			reply.Err = commandContext.reply.Err
			reply.Value = commandContext.reply.Value
			kv.mu.Unlock()
			return
		}
	}
	//2.若命令未被执行,那么开始生成Op并传递给raft
	op := Op{
		CommandType: GetMethod,
		Key:         args.Key,
		ClientId:    args.ClientId,
		CommandId:   args.CommandId,
	}
	index, term, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg)
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	//4.等待应用后返回消息
	select {
	case replyMsg := <-replyCh:
		//当被通知时,返回结果
		//先判断是否该index处的日志并不是我们这个请求实际的那个log
		if replyMsg.Term > term {
			reply.Err = ErrTimeout
		} else {
			reply.Err = replyMsg.Err
			reply.Value = replyMsg.Value
		}
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	//5.清除chan
	go kv.CloseChan(index)
}

func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.replyChMap, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//defer kv.mu.Unlock()
	defer func() {
		log.Printf("kvserver[%d]: 返回PutAppend RPC请求,args=[%v];reply=[%v]\n", kv.me, args, reply)
	}()
	log.Printf("kvserver[%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.me, args)
	//1.先判断该命令是否已经被执行过了
	//1.先判断该命令是否已经被执行过了
	if commandContext, ok := kv.clientReply[args.ClientId]; ok {
		if commandContext.commandId >= args.CommandId {
			//若当前的请求已经被执行过了,那么直接返回结果
			reply.Err = commandContext.reply.Err
			kv.mu.Unlock()
			return
		}
	}
	//2.若命令未被执行,那么开始生成Op并传递给raft
	op := Op{
		CommandType: CommandType(args.Op),
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		CommandId:   args.CommandId,
	}
	index, term, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg)
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	//4.等待应用后返回消息
	select {
	case replyMsg := <-replyCh:
		//当被通知时,返回结果
		//先判断是否该index处的日志并不是我们这个请求实际的那个log
		if replyMsg.Term > term {
			reply.Err = ErrTimeout
		} else {
			reply.Err = replyMsg.Err
		}
	case <-time.After(500 * time.Millisecond):
		//超时,返回结果,但是不更新Command -> Reply
		reply.Err = ErrWrongLeader
	}
	go kv.CloseChan(index)
}

func (kv *KVServer) ApplyCommand() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		log.Printf("kvserver[%d]: 获取到applyCh中新的applyMsg=[%v]\n", kv.me, applyMsg)
		kv.mu.Lock()
		var commonReply ApplyNotifyMsg
		//当为合法命令时
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			index := applyMsg.CommandIndex
			//当命令已经被应用过了(不管该命令了,让server那边超时即可)
			if commandContext, ok := kv.clientReply[op.ClientId]; ok && commandContext.commandId >= op.CommandId {
				kv.mu.Unlock()
				continue
			}
			//当命令未被应用过
			if op.CommandType == GetMethod {
				//Get请求时
				if value, ok := kv.kvData[op.Key]; ok {
					//有该数据时
					commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
				} else {
					//当没有数据时
					commonReply = ApplyNotifyMsg{ErrNoKey, "", applyMsg.CommandTerm}
				}
			} else if op.CommandType == PutMethod {
				//Put请求时
				kv.kvData[op.Key] = op.Value
				commonReply = ApplyNotifyMsg{OK, op.Value, applyMsg.CommandTerm}
			} else if op.CommandType == AppendMethod {
				//Append请求时
				if value, ok := kv.kvData[op.Key]; ok {
					//若已有该key,那么append就是拼接到value上
					newValue := value + op.Value
					kv.kvData[op.Key] = newValue
					commonReply = ApplyNotifyMsg{OK, newValue, applyMsg.CommandTerm}
				} else {
					//若没有key,则效果和put相同
					kv.kvData[op.Key] = op.Value
					commonReply = ApplyNotifyMsg{OK, op.Value, applyMsg.CommandTerm}
				}
			}
			//通知handler去响应请求
			if replyCh, ok := kv.replyChMap[index]; ok {
				replyCh <- commonReply
			}
			log.Printf("kvserver[%d]: 此时key=[%v],value=[%v]\n", kv.me, op.Key, kv.kvData[op.Key])
			//更新clientReply
			kv.clientReply[op.ClientId] = CommandContext{op.CommandId, commonReply}
			log.Printf("kvserver[%d]: 更新ClientId=[%d],CommandId=[%d],Reply=[%v]\n", kv.me, op.ClientId, op.CommandId, commonReply)
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.clientReply = make(map[int64]CommandContext)
	kv.kvData = make(map[string]string)
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	go kv.ApplyCommand()
	return kv
}
