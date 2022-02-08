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
	lastLeader int //上一次RPC发现的主机id
	mu         sync.Mutex
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
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{
		Key:       key,
		CommandId: nrand(),
	}
	reply := GetReply{}
	//log.Printf("client: 开始发送Get RPC;args=[%v]\n", args)
	//第一个发送的目标server是上一次RPC发现的leader
	serverId := ck.lastLeader
	serverNum := len(ck.servers)
	for ; ; serverId++ {
		ok := ck.servers[serverId%serverNum].Call("KVServer.Get", &args, &reply)
		//当发送失败或者返回不是leader时,则继续到下一个server进行尝试
		if !ok || reply.Err == ErrWrongLeader {
			continue
		}
		//log.Printf("client: 发送Get RPC;args=[%v]到server[%d]成功,reply=[%v]\n", args, serverId, reply)
		//若发送成功,则更新最近发现的leader
		ck.mu.Lock()
		ck.lastLeader = serverId
		ck.mu.Unlock()
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	//fmt.Println("key=", key, "value=", value, "op=", op)
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		CommandId: nrand(),
	}
	reply := PutAppendReply{}
	//第一个发送的目标server是上一次RPC发现的leader
	//log.Printf("client: 开始发送PutAppend RPC;args=[%v]\n", args)
	serverId := ck.lastLeader
	serverNum := len(ck.servers)
	for ; ; serverId++ {
		ok := ck.servers[serverId%serverNum].Call("KVServer.PutAppend", &args, &reply)
		//当发送失败或者返回不是leader时,则继续到下一个server进行尝试
		if !ok || reply.Err == ErrWrongLeader {
			continue
		}
		//log.Printf("client: 发送PutAppend RPC;args=[%v]到server[%d]成功,reply=[%v]\n", args, serverId, reply)
		//若发送成功,则更新最近发现的leader
		ck.mu.Lock()
		ck.lastLeader = serverId
		ck.mu.Unlock()
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
