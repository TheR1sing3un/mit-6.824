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
	CommandType CommandType //指令类型(put/append/get)
	Key         string      //键
	Value       string      //值(Get请求时此处为空)
	ClientId    int64       //client的唯一id
	RequestId   int         //请求的唯一id
	Shard       int         //分片迁移命令的分片id
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
	clientSeq      map[int64]RequestResult     //客户端的唯一指令和响应的请求id以及read操作读到的内容
	kvDataBase     KvDataBase                  //数据库,可自行定义和更换
	storeInterface store                       //数据库接口
	replyChMap     map[int]chan ApplyNotifyMsg //某index的响应的chan
	lastApplied    int                         //上一条应用的log的index,防止快照导致回退
	mck            *shardctrler.Clerk          //和shardctrler交互
	config         shardctrler.Config          //当前的配置
	dead           int32                       //是否已关闭

	needSendShards   map[int]map[int]map[string]string //configNum -> (shard -> data)
	needPullShards   map[int]int                       //shard -> configNum, 表示当前需要从哪个配置列表中拉取shard
	shardsAcceptable map[int]bool                      //shard -> acceptable about this group to handle target shard
}

// ApplyNotifyMsg 可表示GetReply和PutAppendReply
type ApplyNotifyMsg struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
	//该被应用的command的term,便于RPC handler判断是否为过期请求(之前为leader并且start了,但是后来没有成功commit就变成了follower,导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
	//或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader的commit的log了
	Term int
}

type RequestResult struct {
	RequestId int    //为了去重
	Err       Err    //读请求时需要返回的err(因为读请求可能为ErrNoKey)
	Value     string //读到的值(Append和Put的历史请求直接返回OK即可,但是读请求需要返回历史请求结果)
}

//该集群负责该分片
func (kv *ShardKV) responsibleForShard(shard int) bool {
	gId := kv.config.Shards[shard]
	return gId == kv.gid
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer func() {
		DPrintf("shardkv[%d][%d]: 返回Get RPC请求,args=[%v];Reply=[%v]\n", kv.gid, kv.me, args, reply)
	}()
	DPrintf("shardkv[%d][%d]: 接收Get RPC请求,args=[%v]\n", kv.gid, kv.me, args)
	if _, ok := kv.shardsAcceptable[key2shard(args.Key)]; !ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	//判断该命令是否已经被执行过了
	if result, ok := kv.clientSeq[args.ClientId]; ok {
		//若该命令已被执行了,直接返回刚刚返回的结果
		if result.RequestId >= args.RequestId {
			//若当前的请求已经被执行过了,那么直接返回结果
			reply.Err = OK
			reply.Value = result.Value
			DPrintf("shardkv[%d][%d]: 请求requestId = [%d]已被执行过直接返回,当前requestId = [%d],Key = [%v], Value = [%v]\n", kv.gid, kv.me, args.RequestId, result.RequestId, args.Key, result.Value)
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
		RequestId:   args.RequestId,
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
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
			reply.Value = replyMsg.Value
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

func (kv *ShardKV) responsibleForKey(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer func() {
		DPrintf("shardkv[%d][%d]: 返回PutAppend RPC请求,args=[%v];Reply=[%v]\n", kv.gid, kv.me, args, reply)
	}()
	DPrintf("shardkv[%d][%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.gid, kv.me, args)
	if _, ok := kv.shardsAcceptable[key2shard(args.Key)]; !ok {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	//判断该命令是否已经被执行过了
	if result, ok := kv.clientSeq[args.ClientId]; ok {
		//若该命令已被执行了,直接返回刚刚返回的结果
		if result.RequestId >= args.RequestId {
			//若当前的请求已经被执行过了,那么直接返回结果
			reply.Err = OK
			DPrintf("shardkv[%d][%d]: 请求requestId = [%d]已被执行过直接返回,当前requestId = [%d]\n", kv.gid, kv.me, args.RequestId, result.RequestId)
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
		RequestId:   args.RequestId,
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
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
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
	defer kv.mu.Unlock()
	DPrintf("shardkv[%d][%d]: 接收到shard=[%d],configNum=[%d]转移请求\n", kv.gid, kv.me, args.Shard, args.ConfigNum)
	defer func() {
		DPrintf("shardkv[%d][%d]: 返回ShardMove RPC请求,args=[%v];Reply=[%v]\n", kv.gid, kv.me, args, reply)
	}()
	//若请求的配置编号大于当前自己的配置编号,不能进行分片转移
	if args.ConfigNum >= kv.config.Num {
		DPrintf("shardkv[%d][%d]: shard=[%d],configNum=[%d]转移请求大于自身当前的config=[%d]\n", kv.gid, kv.me, args.Shard, args.ConfigNum, kv.config.Num)
		reply.Err = ErrServerNoUpdated
		return
	}
	//返回
	reply.ConfigNum, reply.Shard = args.ConfigNum, args.Shard
	reply.Data, reply.ClientSeq = kv.deepCopyDataAndClientSeq(args.ConfigNum, args.Shard)
	reply.Err = OK
	return
}

func (kv *ShardKV) deepCopyDataAndClientSeq(configNum int, shard int) (data map[string]string, clientSeq map[int64]RequestResult) {
	data = make(map[string]string)
	clientSeq = make(map[int64]RequestResult)
	for k, v := range kv.clientSeq {
		clientSeq[k] = v
	}
	//根据config编号以及分片id获得目标config下需要发出去的分片数据
	for k, v := range kv.needSendShards[configNum][shard] {
		data[k] = v
	}
	return data, clientSeq
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
	var clientSeq map[int64]RequestResult
	var config shardctrler.Config
	var needSendShards map[int]map[int]map[string]string
	var needPullShards map[int]int
	var shardsAcceptable map[int]bool
	if d.Decode(&kvDataBase) != nil || d.Decode(&clientSeq) != nil || d.Decode(&config) != nil || d.Decode(&needSendShards) != nil ||
		d.Decode(&needPullShards) != nil || d.Decode(&shardsAcceptable) != nil {
		DPrintf("shardkv[%d][%d]: decode error\n", kv.gid, kv.me)
	} else {
		kv.kvDataBase = kvDataBase
		kv.storeInterface = &kvDataBase
		kv.clientSeq = clientSeq
		kv.config = config
		kv.needSendShards = needSendShards
		kv.needPullShards = needPullShards
		kv.shardsAcceptable = shardsAcceptable
	}
}

func (kv *ShardKV) getNextConfig(next int) (newConfig shardctrler.Config, ok bool) {
	config := kv.mck.Query(next)
	return config, config.Num == next
}

func (kv *ShardKV) PullConfig() {
	kv.mu.Lock()
	//还有未拉取完的shards
	if len(kv.needPullShards) > 0 {
		kv.mu.Unlock()
		return
	}
	if config, ok := kv.getNextConfig(kv.config.Num + 1); ok {
		kv.mu.Unlock()
		kv.rf.Start(ConfigPushCommand{config})
	} else {
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) PullShards() {
	kv.mu.Lock()
	//在需要拉取的分片中遍历
	wg := sync.WaitGroup{}
	for shard, configNum := range kv.needPullShards {
		DPrintf("shardkv[%d][%d]: 开始从configNum = [%d]中拉取shard = [%d]\n", kv.gid, kv.me, configNum, shard)
		c := kv.mck.Query(configNum)
		DPrintf("shardkv[%d][%d]: 查询到configNum = [%d]的config = %v\n", kv.gid, kv.me, configNum, c)
		wg.Add(1)
		go func(shard int, config shardctrler.Config) {
			defer wg.Done()
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				return
			}
			gId := config.Shards[shard]
			args := ShardMoveArgs{shard, config.Num}
			for _, server := range config.Groups[gId] {
				srv := kv.make_end(server)
				reply := ShardMoveReply{}
				DPrintf("shardkv[%d][%d]: 向server[%v]请求拉取configNum = [%d],shard = [%d]\n", kv.gid, kv.me, server, config.Num, shard)
				ok := srv.Call("ShardKV.ShardMove", &args, &reply)
				DPrintf("shardkv[%d][%d]: 向server[%v]请求拉取configNum = [%d],shard = [%d]返回结果: %v\n", kv.gid, kv.me, server, config.Num, shard, reply)
				if ok && reply.Err == OK {
					DPrintf("shardkv[%d][%d]: 拉取成功configNum = [%d]中拉取shard = [%d]\n", kv.gid, kv.me, config.Num, shard)
					kv.rf.Start(ShardReplicaCommand{reply.ConfigNum, reply.Shard, reply.Data, reply.ClientSeq})
					break
				}
			}
		}(shard, c)
	}
	kv.mu.Unlock()
	wg.Wait()
	DPrintf("shardkv[%d][%d]: 结束一轮shards拉取\n", kv.gid, kv.me)
}
func mergeClientReply(seq1 map[int64]RequestResult, seq2 map[int64]RequestResult) map[int64]RequestResult {
	for clientId, result2 := range seq2 {
		if result1, ok := seq1[clientId]; !ok {
			seq1[clientId] = result2
		} else {
			if result1.RequestId > result2.RequestId {
				seq1[clientId] = result1
			} else {
				seq1[clientId] = result2
			}
		}
	}
	return seq1
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

//更新配置
func (kv *ShardKV) updateConfig(config shardctrler.Config) {
	//若接受到的配置小于当前配置,则直接结束(只接收新的config)
	if config.Num <= kv.config.Num {
		DPrintf("shardkv[%d][%d]: 旧配置 %d,当前配置已经为: %v\n", kv.gid, kv.me, config.Num, kv.config.Num)
		return
	}
	oldConfig := kv.config
	needSendShards := kv.shardsAcceptable
	//更新配置
	kv.config = config
	//更新当前配置可接收的shards
	kv.shardsAcceptable = make(map[int]bool)
	for shard, gId := range config.Shards {
		if gId != kv.gid {
			//若新配置的该shard的负责组不是自身,则跳过
			continue
		}
		if _, ok := needSendShards[shard]; ok || oldConfig.Num == 0 {
			//当前仍对该节点分片负责,因为gid指向本组
			kv.shardsAcceptable[shard] = true
			//从需要发送给别的组的分分片集合中删掉当前仍负责的分片,因为上一个配置也是自己负责的,因为不需要发送给别的节点
			delete(needSendShards, shard)
		} else {
			//否则,自己需要记录一下需要拉取的分片以及旧配置编号(拉取的时候,可以进行查询后找到旧配置中负责该shard的server,从而拉取目标shard)
			kv.needPullShards[shard] = oldConfig.Num
		}
	}
	//记录哪些是需要发出去的
	if len(needSendShards) > 0 {
		for shard := range needSendShards {
			//根据shard获取响应数据
			data := make(map[string]string)
			for k, v := range kv.kvDataBase.KvData {
				if key2shard(k) == shard {
					//若该key在分片中
					data[k] = v
					//删掉数据库中的该数据(不会再对该分片负责,因此可以删掉,若需要发送分片,则数据是从needSendShards中取得)
					delete(kv.kvDataBase.KvData, k)
				}
			}
			//记录下来需要发送的数据
			if shardData, ok := kv.needSendShards[oldConfig.Num]; !ok {
				shardData = make(map[int]map[string]string)
				shardData[shard] = data
				kv.needSendShards[oldConfig.Num] = shardData
			} else {
				shardData[shard] = data
			}
		}
	}
	DPrintf("shardkv[%d][%d]: 成功更新到config: %v\n", kv.gid, kv.me, kv.config)
}

//接收分片和去重列表
func (kv *ShardKV) replicaShards(cmd ShardReplicaCommand) {
	if cmd.ConfigNum != kv.config.Num-1 {
		DPrintf("shardkv[%d][%d]: 旧配置的data和clientSeq复制命令 %d,当前配置已经为: %v\n", kv.gid, kv.me, cmd.ConfigNum, kv.config.Num)
		return
	}
	if _, ok := kv.needPullShards[cmd.Shard]; !ok {
		DPrintf("shardkv[%d][%d]: 旧配置的data和clientSeq复制命令 %d,当前配置已经为: %v\n", kv.gid, kv.me, cmd.ConfigNum, kv.config.Num)
		return
	}
	//删掉该需要拉取的分片记录
	delete(kv.needPullShards, cmd.Shard)
	if _, ok := kv.shardsAcceptable[cmd.Shard]; !ok {
		//若当前的可接受分片列表中没有该分片,那么就是需要进行接收并且转化为对其负责
		kv.storeInterface.Merge(cmd.Data)
		mergeClientReply(kv.clientSeq, cmd.ClientSeq)
		//对该分片负责
		kv.shardsAcceptable[cmd.Shard] = true
		DPrintf("shardkv[%d][%d]: 成功复制了shard: %v,来自configNum: %v\n", kv.gid, kv.me, cmd.Shard, cmd.ConfigNum)
	}
}

func (kv *ShardKV) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if applyMsg.CommandIndex <= kv.lastApplied {
		return
	}
	kv.lastApplied = applyMsg.CommandIndex
	if cmd, ok := applyMsg.Command.(ShardReplicaCommand); ok {
		DPrintf("shardkv[%d][%d]: 该命令为ShardReplicaCommand: %v\n", kv.gid, kv.me, cmd)
		kv.replicaShards(cmd)
	} else if cmd, ok := applyMsg.Command.(ConfigPushCommand); ok {
		DPrintf("shardkv[%d][%d]: 该命令为ConfigPushCommand: %v\n", kv.gid, kv.me, cmd)
		kv.updateConfig(cmd.Config)
	} else {
		var commonReply ApplyNotifyMsg
		op := applyMsg.Command.(Op)
		index := applyMsg.CommandIndex
		//判断是否已经不为该组负责了
		if _, ok := kv.shardsAcceptable[key2shard(op.Key)]; !ok {
			commonReply.Err = ErrWrongGroup
			//通知handler去响应请求
			if replyCh, ok := kv.replyChMap[index]; ok {
				DPrintf("shardkv[%d][%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.gid, kv.me, applyMsg, index)
				replyCh <- commonReply
				DPrintf("shardkv[%d][%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.gid, kv.me, applyMsg, index)
			}
		} else {
			//当命令已经被应用过了
			if result, ok := kv.clientSeq[op.ClientId]; ok && result.RequestId >= op.RequestId {
				DPrintf("shardkv[%d][%d]: 该命令已被应用过,applyMsg: %v, requestId: %v\n", kv.gid, kv.me, applyMsg, op.RequestId)
			} else {
				//当命令未被应用过
				switch op.CommandType {
				case GetMethod:
					//Get请求时
					if value, ok := kv.storeInterface.Get(op.Key); ok {
						//有该数据时
						commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
					} else {
						//当没有数据时
						commonReply = ApplyNotifyMsg{ErrNoKey, value, applyMsg.CommandTerm}
					}
				case PutMethod:
					//Put请求时
					value := kv.storeInterface.Put(op.Key, op.Value)
					commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
				case AppendMethod:
					//Append请求时
					newValue := kv.storeInterface.Append(op.Key, op.Value)
					commonReply = ApplyNotifyMsg{OK, newValue, applyMsg.CommandTerm}
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
				kv.clientSeq[op.ClientId] = RequestResult{op.RequestId, commonReply.Err, commonReply.Value}
				DPrintf("shardkv[%d][%d]: 更新ClientId=[%d],RequestId=[%d],Reply=[%v]\n", kv.gid, kv.me, op.ClientId, op.RequestId, commonReply)
			}
		}
	}
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
	err = e.Encode(kv.clientSeq)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode clientSeq error: %v\n", kv.gid, kv.me, err)
	}
	//编码config
	err = e.Encode(kv.config)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode config error: %v\n", kv.gid, kv.me, err)
	}
	//编码needSendShards
	err = e.Encode(kv.needSendShards)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode needSendShards error: %v\n", kv.gid, kv.me, err)
	}
	//编码needPullShards
	err = e.Encode(kv.needPullShards)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode needPullShards error: %v\n", kv.gid, kv.me, err)
	}
	//编码shardsAcceptable
	err = e.Encode(kv.shardsAcceptable)
	if err != nil {
		log.Fatalf("shardkv[%d][%d]: encode shardsAcceptable error: %v\n", kv.gid, kv.me, err)
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
	labgob.Register(ConfigPushCommand{})
	labgob.Register(ShardReplicaCommand{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.clientSeq = make(map[int64]RequestResult)
	kv.kvDataBase = KvDataBase{make(map[string]string)}
	kv.storeInterface = &kv.kvDataBase
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	kv.needSendShards = make(map[int]map[int]map[string]string)
	kv.needPullShards = make(map[int]int)
	kv.shardsAcceptable = make(map[int]bool)
	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	//从快照中恢复数据
	kv.readSnapshot(kv.rf.GetSnapshot())
	go kv.ReceiveApplyMsg()
	go kv.Ticker(kv.PullConfig, 50)
	go kv.Ticker(kv.PullShards, 30)
	return kv
}

type store interface {
	Get(key string) (value string, ok bool)
	Put(key string, value string) (newValue string)
	Append(key string, arg string) (newValue string)
	Merge(map[string]string)
}

func (kv *ShardKV) Ticker(fn func(), timeout int) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			fn()
		}
		time.Sleep(time.Duration(timeout) * time.Millisecond)
	}
}
