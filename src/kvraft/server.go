package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
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
	CommandType CommandType      //指令类型(put/append/get)
	Key         string           //键
	Value       string           //值(Get请求时此处为空)
	ReplyCh     chan CommonReply //当命令未被Apply时,RPC在该Cond上等待,被Apply后则Signal
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	opReply map[int64]CommonReply //客户端的唯一指令和响应的对应map
	kvData  map[string]string     //kv数据
}

// CommonReply 可表示GetReply和PutAppendReply
type CommonReply struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer func() {
		//log.Printf("kvserver[%d]: 返回Get RPC请求,args=[%v];reply=[%v]\n", kv.me, args, reply)
	}()
	//log.Printf("kvserver[%d]: 接收Get RPC请求,args=[%v]\n", kv.me, args)
	//1.先判断该命令是否已经被执行过了
	if commonReply, ok := kv.opReply[args.CommandId]; ok {
		//当存在该命令时(即已经被执行过了,那么直接返回响应)
		reply.Err = commonReply.Err
		reply.Value = commonReply.Value
		return
	}
	//2.若命令未被执行,那么开始生成Op并传递给raft
	op := Op{
		CommandType: GetMethod,
		Key:         args.Key,
		ReplyCh:     make(chan CommonReply),
	}
	_, _, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Unlock()
	//4.等待应用后返回消息
	replyMsg := <-op.ReplyCh
	//5.当被通知时,返回结果
	reply.Err = replyMsg.Err
	reply.Value = replyMsg.Value
	//6.更新CommandId -> Reply
	kv.mu.Lock()
	kv.opReply[args.CommandId] = CommonReply{reply.Err, reply.Value}
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	defer func() {
		//log.Printf("kvserver[%d]: 返回PutAppend RPC请求,args=[%v];reply=[%v]\n", kv.me, args, reply)
	}()
	//log.Printf("kvserver[%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.me, args)
	//1.先判断该命令是否已经被执行过了
	if commonReply, ok := kv.opReply[args.CommandId]; ok {
		//当存在该命令时(即已经被执行过了,那么直接返回响应)
		reply.Err = commonReply.Err
		return
	}
	//2.若命令未被执行,那么开始生成Op并传递给raft
	op := Op{
		CommandType: CommandType(args.Op),
		Key:         args.Key,
		Value:       args.Value,
		ReplyCh:     make(chan CommonReply),
	}
	_, _, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Unlock()
	//4.等待应用后返回消息
	replyMsg := <-op.ReplyCh
	//5.当被通知时,返回结果
	reply.Err = replyMsg.Err
	//6.更新CommandId -> Reply
	kv.mu.Lock()
	kv.opReply[args.CommandId] = CommonReply{reply.Err, ""}
	return
}

func (kv *KVServer) ApplyCommand() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		//log.Printf("kvserver[%d]: 获取到applyCh中新的applyMsg=[%v]\n", kv.me, applyMsg)
		kv.mu.Lock()
		//当为合法命令时
		if applyMsg.CommandValid {
			op := applyMsg.Command.(Op)
			if op.CommandType == GetMethod {
				//Get请求时
				if value, ok := kv.kvData[op.Key]; ok {
					//有该数据时
					if op.ReplyCh != nil {
						op.ReplyCh <- CommonReply{OK, value}
					}
				} else {
					//当没有数据时
					if op.ReplyCh != nil {
						op.ReplyCh <- CommonReply{ErrNoKey, ""}
					}
				}
			} else if op.CommandType == PutMethod {
				//Put请求时
				kv.kvData[op.Key] = op.Value
				if op.ReplyCh != nil {
					op.ReplyCh <- CommonReply{OK, op.Value}
				}
			} else if op.CommandType == AppendMethod {
				//Append请求时
				if value, ok := kv.kvData[op.Key]; ok {
					//若已有该key,那么append就是拼接到value上
					kv.kvData[op.Key] = value + op.Value
					if op.ReplyCh != nil {
						op.ReplyCh <- CommonReply{OK, value + op.Value}
					}
				} else {
					//若没有key,则效果和put相同
					kv.kvData[op.Key] = op.Value
					if op.ReplyCh != nil {
						op.ReplyCh <- CommonReply{OK, op.Value}
					}
				}
			}
			//log.Printf("kvserver[%d]: 此时key=[%v],value=[%v]\n", kv.me, op.Key, kv.kvData[op.Key])
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
	kv.opReply = make(map[int64]CommonReply)
	kv.kvData = make(map[string]string)
	go kv.ApplyCommand()
	return kv
}
