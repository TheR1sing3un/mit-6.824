package shardctrler

//
// Shardctrler clerk.
//

import "6.824/labrpc"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId  int64 //客户端id
	RequestId int   //当前请求的id
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
	// Your code here.
	ck.ClientId = nrand()
	ck.RequestId = 0
	return ck
}

func (ck *Clerk) Query(num int) (config Config) {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.ClientId
	args.RequestId = ck.RequestId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.RequestId++
				config = reply.Config
				DPrintf("ShardClient[%d]: Query请求完成,requestId=[%d],reply=[%v]\n", ck.ClientId, args.RequestId, reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.ClientId
	args.RequestId = ck.RequestId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.RequestId++
				DPrintf("ShardClient[%d]: Join请求完成,requestId=[%d],reply=[%v]\n", ck.ClientId, args.RequestId, reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.ClientId
	args.RequestId = ck.RequestId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.RequestId++
				DPrintf("ShardClient[%d]: Leave请求完成,requestId=[%d],reply=[%v]\n", ck.ClientId, args.RequestId, reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.ClientId
	args.RequestId = ck.RequestId
	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false && reply.Err == OK {
				ck.RequestId++
				DPrintf("ShardClient[%d]: Move请求完成,requestId=[%d],reply=[%v]\n", ck.ClientId, args.RequestId, reply)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
