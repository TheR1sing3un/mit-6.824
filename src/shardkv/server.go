package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType CommandType       //指令类型(put/append/get)
	Key         string            //键
	Value       string            //值(Get请求时此处为空)
	ClientId    int64             //client的唯一id
	CommandId   int               //命令的唯一id
	Shard       int               //分片迁移命令的分片id
	Data        map[string]string //移动过来的分片的数据
	Config      shardctrler.Config
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientReply         map[int64]CommandContext    //客户端的唯一指令和响应的对应map
	kvDataBase          KvDataBase                  //数据库,可自行定义和更换
	storeInterface      store                       //数据库接口
	replyChMap          map[int]chan ApplyNotifyMsg //某index的响应的chan
	lastApplied         int                         //上一条应用的log的index,防止快照导致回退
	mck                 *shardctrler.Clerk          //和shardctrler交互
	config              shardctrler.Config          //当前的配置
	dead                int32                       //是否已关闭
	timerUpdateConfig   *time.Timer                 //更新配置的定时器
	timeoutUpdateConfig int                         //更新配置的轮询周期(/ms)
	configState         State                       //配置状态(updating/updated)
	shardsState         [shardctrler.NShards]State
}

// ApplyNotifyMsg 可表示GetReply和PutAppendReply
type ApplyNotifyMsg struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
	//该被应用的command的term,便于RPC handler判断是否为过期请求(之前为leader并且start了,但是后来没有成功commit就变成了follower,导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
	//或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader的commit的log了
	Term int
	Data map[string]string //目标分片的数据
}

type CommandContext struct {
	Command int            //该client的目前的commandId
	Reply   ApplyNotifyMsg //该command的响应
}

//该集群对该分片
func (kv *ShardKV) responsibleForShard(shard int) bool {
	gId := kv.config.Shards[shard]
	return gId == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// Your code here.
	kv.mu.Lock()
	defer func() {
		DPrintf("shardkv[%d][%d]: 返回Get RPC请求,args=[%v];Reply=[%v]\n", kv.gid, kv.me, args, reply)
	}()
	DPrintf("shardkv[%d][%d]: 接收Get RPC请求,args=[%v]\n", kv.gid, kv.me, args)
	shard := key2shard(args.Key)
	if !kv.responsibleForShard(shard) {
		//若该副本组不负责该key
		reply.Err = ErrWrongGroup
		DPrintf("shardkv[%d][%d]: 该副本组不负责该key=[%v],shard=[%d],shard belong to: [%d]\n", kv.gid, kv.me, args.Key, shard, kv.config.Shards[shard])
		kv.mu.Unlock()
		return
	}
	//1.先判断该命令是否已经被执行过了
	if commandContext, ok := kv.clientReply[args.ClientId]; ok {
		if commandContext.Command >= args.RequestId {
			//若当前的请求已经被执行过了,那么直接返回结果
			reply.Err = commandContext.Reply.Err
			reply.Value = commandContext.Reply.Value
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	//2.若命令未被执行,那么开始生成Op并传递给raft
	op := Op{
		CommandType: GetMethod,
		Key:         args.Key,
		ClientId:    args.ClientId,
		CommandId:   args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	DPrintf("shardkv[%d][%d]: 创建reply通道:index=[%d]\n", kv.gid, kv.me, index)
	kv.mu.Unlock()
	//4.等待应用后返回消息
	select {
	case replyMsg := <-replyCh:
		//当被通知时,返回结果
		DPrintf("shardkv[%d][%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", kv.gid, kv.me, index, replyMsg)
		if term == replyMsg.Term && key2shard(args.Key) == kv.gid {
			reply.Err = replyMsg.Err
			reply.Value = replyMsg.Value
		} else if key2shard(args.Key) != kv.gid {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("shardkv[%d][%d]: 处理请求超时: %v\n", kv.gid, kv.me, op)
		reply.Err = ErrTimeout
	}
	//5.清除chan
	go kv.CloseChan(index)
}
func (kv *ShardKV) CloseChan(index int) {
	kv.mu.Lock()
	//DPrintf("shardkv[%d][%d]: 开始删除通道index: %d\n", kv.me, index)
	defer kv.mu.Unlock()
	ch, ok := kv.replyChMap[index]
	if !ok {
		//若该index没有保存通道,直接结束
		DPrintf("shardkv[%d][%d]: 无该通道index: %d\n", kv.gid, kv.me, index)
		return
	}
	close(ch)
	delete(kv.replyChMap, index)
	DPrintf("shardkv[%d][%d]: 成功删除通道index: %d\n", kv.gid, kv.me, index)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Your code here.
	kv.mu.Lock()
	//defer kv.mu.Unlock()
	defer func() {
		DPrintf("shardkv[%d][%d]: 返回PutAppend RPC请求,args=[%v];Reply=[%v]\n", kv.gid, kv.me, args, reply)
	}()
	DPrintf("shardkv[%d][%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.gid, kv.me, args)
	//if args.ConfigNum != kv.config.Num {
	//	reply.Err = ErrValidConfig
	//	kv.mu.Unlock()
	//	return
	//}
	shard := key2shard(args.Key)
	if !kv.responsibleForShard(shard) {
		//若该副本组不负责该key
		reply.Err = ErrWrongGroup
		DPrintf("shardkv[%d][%d]: 该副本组不负责该key=[%v],shard=[%d],shard belong to: [%d]\n", kv.gid, kv.me, args.Key, shard, kv.config.Shards[shard])
		kv.mu.Unlock()
		return
	}
	//1.先判断该命令是否已经被执行过了
	if commandContext, ok := kv.clientReply[args.ClientId]; ok {
		//若该命令已被执行了,直接返回刚刚返回的结果
		if commandContext.Command == args.RequestId {
			//若当前的请求已经被执行过了,那么直接返回结果
			reply.Err = commandContext.Reply.Err
			DPrintf("shardkv[%d][%d]: CommandId=[%d]==CommandContext.CommandId=[%d] ,直接返回: %v\n", kv.gid, kv.me, args.RequestId, commandContext.Command, reply)
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	//2.若命令未被执行,那么开始生成Op并传递给raft
	op := Op{
		CommandType: CommandType(args.Op),
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		CommandId:   args.RequestId,
	}
	index, term, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	DPrintf("shardkv[%d][%d]: 创建reply通道:index=[%d]\n", kv.gid, kv.me, index)
	kv.mu.Unlock()
	//4.等待应用后返回消息
	select {
	case replyMsg := <-replyCh:
		//当被通知时,返回结果
		if term == replyMsg.Term && key2shard(args.Key) == kv.gid {
			reply.Err = replyMsg.Err
		} else if key2shard(args.Key) != kv.gid {
			reply.Err = ErrWrongGroup
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		//超时,返回结果,但是不更新Command -> Reply
		reply.Err = ErrTimeout
		DPrintf("shardkv[%d][%d]: 处理请求超时: %v\n", kv.gid, kv.me, op)
	}
	go kv.CloseChan(index)
}

// ShardMove 分片转移
func (kv *ShardKV) ShardMove(args *ShardMoveArgs, reply *ShardMoveReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	DPrintf("shardkv[%d][%d]: 接收到shard=[%d],configNum=[%d]转移请求\n", kv.gid, kv.me, args.Shard, args.ConfigNum)
	defer func() {
		DPrintf("shardkv[%d][%d]: 返回ShardMove RPC请求,args=[%v];Reply=[%v]\n", kv.gid, kv.me, args, reply)
	}()
	//若请求的配置编号大于当前自己的配置编号,自己立马去更新配置
	if args.ConfigNum > kv.config.Num {
		if !kv.timerUpdateConfig.Stop() {
			kv.timerUpdateConfig.Reset(0)
		}
	}
	if (kv.shardsState[args.Shard] == Updated && args.ConfigNum > kv.config.Num+1) || (kv.shardsState[args.Shard] == Updating && args.ConfigNum != kv.config.Num) {
		reply.Err = ErrUpdatingShard
		DPrintf("shardkv[%d][%d]: 请求的shard=[%d]正在更新,当前configNum=[%d]\n", kv.gid, kv.me, args.Shard, args.ConfigNum)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	//生成命令
	op := Op{}
	op.CommandType = ShardMoveMethod
	op.Shard = args.Shard
	index, term, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	DPrintf("shardkv[%d][%d]: 创建reply通道:index=[%d]\n", kv.gid, kv.me, index)
	kv.mu.Unlock()
	//4.等待应用后返回消息
	select {
	case replyMsg := <-replyCh:
		//当被通知时,返回结果
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
			reply.Data = replyMsg.Data
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		//超时,返回结果,但是不更新Command -> Reply
		reply.Err = ErrTimeout
		DPrintf("shardkv[%d][%d]: 处理请求超时: %v\n", kv.gid, kv.me, op)
	}
	go kv.CloseChan(index)
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvDataBase KvDataBase
	var clientReply map[int64]CommandContext
	var config shardctrler.Config
	var shardsState [10]State
	if d.Decode(&kvDataBase) != nil || d.Decode(&clientReply) != nil || d.Decode(&config) != nil || d.Decode(&shardsState) != nil {
		DPrintf("shardkv[%d][%d]: decode error\n", kv.gid, kv.me)
	} else {
		kv.kvDataBase = kvDataBase
		kv.storeInterface = &kvDataBase
		kv.clientReply = clientReply
		kv.config = config
		kv.shardsState = shardsState
	}
}

// UpdateConfig 轮询更新配置
func (kv *ShardKV) UpdateConfig() {
	for !kv.killed() {
		select {
		case <-kv.timerUpdateConfig.C:
			//若不为leader则不进行配置拉取
			if _, isLeader := kv.rf.GetState(); !isLeader {
				kv.timerUpdateConfig.Reset(time.Duration(kv.timeoutUpdateConfig) * time.Millisecond)
				//DPrintf("shardkv[%d][%d]: 不为leader,不进行配置拉取\n", kv.gid, kv.me)
				continue
			}
			kv.mu.Lock()
			//查询下一个配置
			newConfig := kv.mck.Query(kv.config.Num + 1)
			//检查是否为新配置
			if newConfig.Num > kv.config.Num {
				//更新配置状态
				kv.configState = Updating
				oldConfig := kv.config
				data := make(map[string]string)
				if len(oldConfig.Groups) != 0 {
					//找出那些是需要迁移的分片以及它们上一个配置中所在的复制组的id
					shardsAndGIds := kv.findNeedGetShards(newConfig, oldConfig)
					DPrintf("shardkv[%d][%d]: 更新配置需要请求的shard->gid的对应为: %v\n", kv.gid, kv.me, shardsAndGIds)
					//更新状态shards状态
					for shard := range shardsAndGIds {
						kv.shardsState[shard] = Updating
					}
					//获取需要分片的数据
					for shard, gId := range shardsAndGIds {
						data = mergeTwoMap(data, kv.requestShard(shard, newConfig.Num, gId))
					}
				}
				kv.mu.Unlock()
				op := Op{}
				op.CommandType = ShardReplicaMethod
				op.Data = data
				op.Config = newConfig
				index, term, isLeader := kv.rf.Start(op)
				if isLeader {
					replyCh := make(chan ApplyNotifyMsg, 1)
					kv.mu.Lock()
					kv.replyChMap[index] = replyCh
					DPrintf("shardkv[%d][%d]: 创建reply通道:index=[%d]\n", kv.gid, kv.me, index)
					kv.mu.Unlock()
					//4.等待应用后返回消息
					select {
					case replyMsg := <-replyCh:
						//当被通知时,返回结果
						kv.mu.Lock()
						if term == replyMsg.Term {
							//kv.config = newConfig
							//kv.storeInterface.Merge(data)
							DPrintf("shardkv[%d][%d]: 成功更新到该config: %v;old config: %v\n", kv.gid, kv.me, newConfig, oldConfig)
						} else {
							DPrintf("shardkv[%d][%d]: term发生变化,未能更新到该config: %v;old config: %v\n", kv.gid, kv.me, newConfig, oldConfig)
						}
						kv.mu.Unlock()

					case <-time.After(500 * time.Millisecond):
						DPrintf("shardkv[%d][%d]: 更新config处理请求超时: %v\n", kv.gid, kv.me, op)
					}
					go kv.CloseChan(index)
				}
				kv.mu.Lock()
				for shard := range kv.shardsState {
					kv.shardsState[shard] = Updated
				}
			}
			//更新配置状态
			kv.configState = Updated
			kv.timerUpdateConfig.Reset(time.Duration(kv.timeoutUpdateConfig) * time.Millisecond)
			kv.mu.Unlock()
		}
	}
}

func mergeTwoMap(map1 map[string]string, map2 map[string]string) (mapSum map[string]string) {
	mapSum = make(map[string]string)
	for key, value := range map1 {
		mapSum[key] = value
	}
	for key, value := range map2 {
		mapSum[key] = value
	}
	return mapSum
}

func (kv *ShardKV) requestShard(shard int, configNum int, gId int) (data map[string]string) {
	args := ShardMoveArgs{shard, configNum}
	for {
		if servers, ok := kv.config.Groups[gId]; ok {
			for si := 0; si < len(servers); si++ {
				srv := kv.make_end(servers[si])
				var reply ShardMoveReply
				DPrintf("shardkv[%d][%d]: 请求shardMove,shard=[%d],server=[%v]\n", kv.gid, kv.me, shard, si)
				kv.mu.Unlock()
				ok := srv.Call("ShardKV.ShardMove", &args, &reply)
				kv.mu.Lock()
				if ok && reply.Err == OK {
					return reply.Data
				}
				if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
					//若发送失败,或者leader错误,或者超时,则到下一个server处重发请求
					continue
				}
				if reply.Err == ErrUpdatingShard {
					time.Sleep(10 * time.Millisecond)
					si--
					continue
				}
			}
		}
	}
}

//找到需要获取的分片id
func (kv *ShardKV) findNeedGetShards(newConfig shardctrler.Config, oldConfig shardctrler.Config) (shardsAndGIds map[int]int) {
	shardsAndGIds = make(map[int]int)
	for shard, gId := range newConfig.Shards {
		if gId == kv.gid && gId != oldConfig.Shards[shard] {
			//当新的配置的gId为该副本组且旧的配置则不为的时候则找到了需要获取的分片id和之前它的负责副本组id
			shardsAndGIds[shard] = oldConfig.Shards[shard]
		}
	}
	return shardsAndGIds
}

func (kv *ShardKV) ReceiveApplyMsg() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("shardkv[%d][%d]: 获取到applyCh中新的applyMsg=[%v]\n", kv.gid, kv.me, applyMsg)
			//当为合法命令时
			if applyMsg.CommandValid {
				kv.ApplyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				//当为合法快照时
				kv.ApplySnapshot(applyMsg)
			} else {
				//非法消息
				DPrintf("shardkv[%d][%d]: error applyMsg from applyCh: %v\n", kv.gid, kv.me, applyMsg)
			}
		}
	}
}

func (kv *ShardKV) getDataByShard(shard int) map[string]string {
	data := make(map[string]string)
	for key, value := range kv.kvDataBase.KvData {
		if key2shard(key) == shard {
			data[key] = value
		}
	}
	return data
}

func (kv *ShardKV) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var commonReply ApplyNotifyMsg
	op := applyMsg.Command.(Op)
	index := applyMsg.CommandIndex
	//先判断是不是分片迁移命令
	if op.CommandType == ShardMoveMethod {
		if replyCh, ok := kv.replyChMap[index]; ok {
			//获取到该分片的所有数据
			data := kv.getDataByShard(op.Shard)
			commonReply.Err = OK
			commonReply.Data = data
			commonReply.Term = applyMsg.CommandTerm
			//通知handler
			DPrintf("shardkv[%d][%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.gid, kv.me, applyMsg, index)
			replyCh <- commonReply
			DPrintf("shardkv[%d][%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.gid, kv.me, applyMsg, index)
		}
		kv.lastApplied = applyMsg.CommandIndex
		return
	}
	//在判断是不是分片复制命令
	if op.CommandType == ShardReplicaMethod {
		//通知应用该配置
		//通知handler
		if kv.config.Num < op.Config.Num {
			kv.config = op.Config
			kv.storeInterface.Merge(op.Data)
			DPrintf("shardkv[%d][%d]: 成功更新到该config: %v\n", kv.gid, kv.me, kv.config)
			if replyCh, ok := kv.replyChMap[index]; ok {
				commonReply.Term = applyMsg.CommandTerm
				DPrintf("shardkv[%d][%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.gid, kv.me, applyMsg, index)
				replyCh <- commonReply
				DPrintf("shardkv[%d][%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.gid, kv.me, applyMsg, index)
			}
		}
		for i := range kv.shardsState {
			kv.shardsState[i] = Updated
		}
		kv.lastApplied = applyMsg.CommandIndex
		return
	}
	//当命令已经被应用过了
	if commandContext, ok := kv.clientReply[op.ClientId]; ok && commandContext.Command >= op.CommandId {
		DPrintf("shardkv[%d][%d]: 该命令已被应用过,applyMsg: %v, commandContext: %v\n", kv.gid, kv.me, applyMsg, commandContext)
		commonReply = commandContext.Reply
		return
	}
	//当命令未被应用过
	if op.CommandType == GetMethod {
		//Get请求时
		if value, ok := kv.storeInterface.Get(op.Key); ok {
			//有该数据时
			commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm, nil}
		} else {
			//当没有数据时
			commonReply = ApplyNotifyMsg{ErrNoKey, value, applyMsg.CommandTerm, nil}
		}
	} else if op.CommandType == PutMethod {
		//Put请求时
		value := kv.storeInterface.Put(op.Key, op.Value)
		commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm, nil}
	} else if op.CommandType == AppendMethod {
		//Append请求时
		newValue := kv.storeInterface.Append(op.Key, op.Value)
		commonReply = ApplyNotifyMsg{OK, newValue, applyMsg.CommandTerm, nil}
	}
	//通知handler去响应请求
	if replyCh, ok := kv.replyChMap[index]; ok {
		DPrintf("shardkv[%d][%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.gid, kv.me, applyMsg, index)
		replyCh <- commonReply
		DPrintf("shardkv[%d][%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.gid, kv.me, applyMsg, index)
	}
	value, _ := kv.storeInterface.Get(op.Key)
	DPrintf("shardkv[%d][%d]: 此时key=[%v],value=[%v]\n", kv.gid, kv.me, op.Key, value)
	//更新clientReply
	kv.clientReply[op.ClientId] = CommandContext{op.CommandId, commonReply}
	DPrintf("shardkv[%d][%d]: 更新ClientId=[%d],CommandId=[%d],Reply=[%v]\n", kv.gid, kv.me, op.ClientId, op.CommandId, commonReply)
	kv.lastApplied = applyMsg.CommandIndex
	//判断是否需要快照
	if kv.needSnapshot() {
		kv.startSnapshot(applyMsg.CommandIndex)
	}
}

//判断当前是否需要进行snapshot(90%则需要快照)
func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	var proportion float32
	proportion = float32(kv.rf.GetRaftStateSize() / kv.maxraftstate)
	return proportion > 0.9
}

//主动开始snapshot(由leader在maxRaftState不为-1,而且目前接近阈值的时候调用)
func (kv *ShardKV) startSnapshot(index int) {
	DPrintf("shardkv[%d][%d]: 容量接近阈值,进行快照,rateStateSize=[%d],maxRaftState=[%d]\n", kv.gid, kv.me, kv.rf.GetRaftStateSize(), kv.maxraftstate)
	snapshot := kv.createSnapshot()
	DPrintf("shardkv[%d][%d]: 完成service层快照\n", kv.gid, kv.me)
	//通知Raft进行快照
	go kv.rf.Snapshot(index, snapshot)
}

//生成server的状态的snapshot
func (kv *ShardKV) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码kv数据
	err := e.Encode(kv.kvDataBase)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode kvData error: %v\n", kv.gid, kv.me, err)
	}
	//编码clientReply(为了去重)
	err = e.Encode(kv.clientReply)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode clientReply error: %v\n", kv.gid, kv.me, err)
	}
	//编码config
	err = e.Encode(kv.config)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode config error: %v\n", kv.gid, kv.me, err)
	}
	//编码shardsState
	err = e.Encode(kv.shardsState)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode shardsState error: %v\n", kv.gid, kv.me, err)
	}
	snapshotData := w.Bytes()
	return snapshotData
}

// ApplySnapshot 被动应用snapshot
func (kv *ShardKV) ApplySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("shardkv[%d][%d]: 接收到leader的快照\n", kv.gid, kv.me)
	if msg.SnapshotIndex < kv.lastApplied {
		DPrintf("shardkv[%d][%d]: 接收到旧的日志,snapshotIndex=[%d],状态机的lastApplied=[%d]\n", kv.gid, kv.me, msg.SnapshotIndex, kv.lastApplied)
		return
	}
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.lastApplied = msg.SnapshotIndex
		//将快照中的service层数据进行加载
		kv.readSnapshot(msg.Snapshot)
		DPrintf("shardkv[%d][%d]: 完成service层快照\n", kv.gid, kv.me)
	}
}

// StartServer
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.clientReply = make(map[int64]CommandContext)
	kv.kvDataBase = KvDataBase{make(map[string]string)}
	kv.storeInterface = &kv.kvDataBase
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.configState = Updated
	for i := range kv.shardsState {
		kv.shardsState[i] = Updated
	}
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//获取当前最新的配置
	//config := kv.mck.Query(-1)
	//kv.config = config
	kv.timeoutUpdateConfig = 100
	kv.timerUpdateConfig = time.NewTimer(time.Duration(kv.timeoutUpdateConfig) * time.Millisecond)
	//从快照中恢复数据
	kv.readSnapshot(kv.rf.GetSnapshot())
	go kv.ReceiveApplyMsg()
	go kv.UpdateConfig()
	return kv
}

type store interface {
	Get(key string) (value string, ok bool)
	Put(key string, value string) (newValue string)
	Append(key string, arg string) (newValue string)
	Merge(map[string]string)
}
