package shardkv

import (
	"6.824/shardctrler"
	"log"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const Debug = false

func DPrintf(format string, a ...interface{}) {
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

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrTimeout         = "ErrTimeout"
	ErrServerNoUpdated = "ErrServerNoUpdated"
)

type Err string

// PutAppendArgs Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId  int64 //client的唯一id
	RequestId int   //客户端请求的id
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64 //client的唯一id
	RequestId int   //客户端请求的id
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardMoveArgs struct {
	Shard     int //分片id
	ConfigNum int //配置编号
}

type ShardMoveReply struct {
	Err       Err
	ConfigNum int                     //配置的编号
	Shard     int                     //分片的编号
	Data      map[string]string       //该Shard的数据
	ClientSeq map[int64]RequestResult //去重的map
}

type ShardReplicaCommand struct {
	ConfigNum int                     //配置的编号
	Shard     int                     //分片的编号
	Data      map[string]string       //该Shard的数据
	ClientSeq map[int64]RequestResult //去重的map
}

type GCSuccessCommand struct {
	ConfigNum int
	Shard     int
}

type GCCommand struct {
	ConfigNum int
	Shard     int
}

type ConfigPushCommand struct {
	Config shardctrler.Config
}

type PushGCSignalArgs struct {
	ConfigNum int //需要回收的配置编号
	Shard     int //需要回收的分片的配置编号
}

type PushGCSignalReply struct {
	Success bool //是否成功回收相应的数据
}
