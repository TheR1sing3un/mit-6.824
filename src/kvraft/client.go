package kvraft

import (
	"6.824/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	lastLeader           int        //上一次RPC发现的主机id
	mu                   sync.Mutex //锁
	clientId             int64      //client唯一id
	lastAppliedCommandId int        //Command的唯一id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.lastLeader = 0
	ck.clientId = nrand()
	ck.lastAppliedCommandId = 0
	return ck
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &Reply)
//
// the types of args and Reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and Reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	commandId := ck.lastAppliedCommandId + 1
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		CommandId: commandId,
	}
	DPrintf("client[%d]: 开始发送Get RPC;args=[%v]\n", ck.clientId, args)
	//第一个发送的目标server是上一次RPC发现的leader
	serverId := ck.lastLeader
	serverNum := len(ck.servers)
	for ; ; serverId = (serverId + 1) % serverNum {
		var reply GetReply
		DPrintf("client[%d]: 开始发送Get RPC;args=[%v]到server[%d]\n", ck.clientId, args, serverId)
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		//当发送失败或者返回不是leader时,则继续到下一个server进行尝试
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			DPrintf("client[%d]: 发送Get RPC;args=[%v]到server[%d]失败,ok = %v,Reply=[%v]\n", ck.clientId, args, serverId, ok, reply)
			continue
		}
		DPrintf("client[%d]: 发送Get RPC;args=[%v]到server[%d]成功,Reply=[%v]\n", ck.clientId, args, serverId, reply)
		//若发送成功,则更新最近发现的leader
		ck.lastLeader = serverId
		ck.lastAppliedCommandId = commandId
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &Reply)
//
// the types of args and Reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and Reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//fmt.Println("key=", key, "value=", value, "op=", op)
	// You will have to modify this function.
	commandId := ck.lastAppliedCommandId + 1
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		CommandId: commandId,
	}
	//第一个发送的目标server是上一次RPC发现的leader
	DPrintf("client[%d]: 开始发送PutAppend RPC;args=[%v]\n", ck.clientId, args)
	serverId := ck.lastLeader
	serverNum := len(ck.servers)
	for ; ; serverId = (serverId + 1) % serverNum {
		var reply PutAppendReply
		DPrintf("client[%d]: 开始发送PutAppend RPC;args=[%v]到server[%d]\n", ck.clientId, args, serverId)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		//当发送失败或者返回不是leader时,则继续到下一个server进行尝试
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader {
			DPrintf("client[%d]: 发送PutAppend RPC;args=[%v]到server[%d]失败,ok = %v,Reply=[%v]\n", ck.clientId, args, serverId, ok, reply)
			continue
		}
		DPrintf("client[%d]: 发送PutAppend RPC;args=[%v]到server[%d]成功,Reply=[%v]\n", ck.clientId, args, serverId, reply)
		//若发送成功,则更新最近发现的leader以及commandId
		ck.lastLeader = serverId
		ck.lastAppliedCommandId = commandId
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
