package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//定义一个raft状态类
type RaftState int

//枚举Raft状态
const (
	FOLLOWER  RaftState = 0
	CANDIDATE RaftState = 1
	LEADER    RaftState = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state
	currentTerm int         //当前任期
	voteFor     int         //当前任期投给的候选人id(为-1时代表没有投票)
	log         []*LogEntry //日志条目
	//volatile state on all servers
	state       RaftState //当前raft状态
	commitIndex int       //当前log中的最高索引(从0开始,递增)
	lastApplied int       //当前被用到状态机中的日志最高索引(从0开始,递增)
	//volatile state on leader
	nextIndex  []int //发送给每台服务器的下一条日志目录索引(初始值为leader的commitIndex + 1)
	matchIndex []int //每台服务器已知的已被复制的最高日志条目索引

	//自定义参数
	timerHeartBeat   *time.Timer //心跳计时器
	timerElect       *time.Timer //选举计时器
	timeoutHeartBeat int         //心跳频率/ms
	timeoutElect     int         //选举频率/ms
}

//日志条目
type LogEntry struct {
	Command interface{} //日志记录的命令(用于应用服务的命令)
	Term    int         //该日志被接收的时候的Leader任期
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	//fmt.Printf("GetState[%d]:term[%d],isLeader:[%v]\n", rf.me, term, isleader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //候选人的任期号
	CandidateId  int //候选人id
	LastLogIndex int //候选人最近一个Log的index
	LastLogTerm  int //候选人最近一个Log的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //接收到的投票请求的server的term
	VoteGranted bool //是否投票
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//无论如何,返回参数中的term应修改为自己的term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("id[%d].state[%v].term[%d]: 接收到[%d]的选举申请\n", rf.me, rf.state, rf.currentTerm, args.CandidateId)
	defer func() {
		log.Printf("id[%d].state[%v].term[%d]: 给[%d]的选举申请返回%v\n", rf.me, rf.state, rf.currentTerm, args.CandidateId, reply.VoteGranted)
	}()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	//1.如果term < currentTerm则直接拒绝并返回
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}
	//1.如果t > currentTerm,则更新currentTerm,并切换为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
		rf.voteFor = -1
	}
	//3.如果voteFor为空或者为CandidateId,而且日志一样新就投票给他
	currentLastLogIndex := -1
	currentLastLogTerm := -1
	if len(rf.log) > 0 {
		currentLastLogIndex = len(rf.log) - 1
		currentLastLogTerm = rf.log[currentLastLogIndex].Term
	}
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && currentLastLogIndex == args.LastLogIndex && currentLastLogTerm == args.LastLogTerm {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		//重置选举时间
		timeout := rand.Intn(400) + 800
		rf.timerElect.Reset(time.Duration(timeout) * time.Millisecond)
		log.Printf("id[%d].state[%v].term[%d]: 重置选举计时器为[%d]ms\n", rf.me, rf.state, rf.currentTerm, timeout)

	}
}

//日志追加RPC的请求参数
type AppendEntriesArgs struct {
	Term         int        //当前leader的任期
	LeaderId     int        //leader的id,follower可以将client错发给它的请求转发给leader
	PrevLogIndex int        //最新日志前的那一条日志条目的索引
	PrevLogTerm  int        //最新日志前的那一条日志条目的任期
	Entries      []LogEntry //需要被保存的日志条目(为空则为心跳包)
	LeaderCommit int        //leader的commitIndex
}

//日志追加的RPC的返回值
type AppendEntriesReply struct {
	Term    int  //接收者的currentTerm
	Success bool //如果prevLogIndex和prevLogTerm和follower的匹配则返回true
}

//日志追加的RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	log.Printf("id[%d].state[%v].term[%d]: 接收到[%d],term[%d]的日志追加,\n", rf.me, rf.state, rf.currentTerm, args.LeaderId, args.Term)
	//1.判断term是否小于当前任期
	if args.Term < rf.currentTerm {
		log.Printf("id[%d].state[%v].term[%d]: 追加日志的任期%d小于当前任期%d\n", rf.me, rf.state, rf.currentTerm, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}
	//重置选举时间
	timeout := rand.Intn(400) + 800
	rf.timerElect.Reset(time.Duration(timeout) * time.Millisecond)
	log.Printf("id[%d].state[%v].term[%d]: 重置选举计时器为[%d]ms\n", rf.me, rf.state, rf.currentTerm, timeout)
	if args.Term > rf.currentTerm || rf.state != FOLLOWER {
		rf.currentTerm = args.Term
		rf.toFollower()
	}
	reply.Success = true
	//2.日志不匹配
	currentPrevLogIndex := -1
	currentPrevLogTerm := -1
	if len(rf.log) > 0 {
		currentPrevLogIndex = len(rf.log) - 1
		currentPrevLogTerm = rf.log[currentPrevLogIndex].Term
	}
	if args.PrevLogIndex != currentPrevLogIndex || args.PrevLogTerm != currentPrevLogTerm {
		log.Printf("id[%d].state[%v].term[%d]: 追加日志的和现在的日志不匹配\n", rf.me, rf.state, rf.currentTerm)
		reply.Success = false
		return
	}
	//TODO:3.如果一个已经存在的条目和新条目(即刚刚接收到的日志条目)发生了冲突(因为索引相同，任期不同),那么就删除这个已经存在的条目以及它之后的所有条目

	//TODO:4.追加日志中尚未存在的任何新条目

	//TODO:5.如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引(leaderCommit > commitIndex)
	//则把接收者的已知已经提交的最高的日志条目的索引commitIndex
	//重置为 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	//6.判断t > currentTerm,更新currentTerm,成为follower
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//向某server发起appendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timerElect.C:
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			log.Printf("id[%d].state[%v].term[%d]: 选举计时器到期\n", rf.me, rf.state, rf.currentTerm)
			if rf.state != LEADER {
				//当不为leader时,也就是超时了,那么转变为Candidate
				go rf.startElection()
			}
			//重置选举计时器
			timeout := rand.Intn(400) + 800
			rf.timerElect.Reset(time.Duration(timeout) * time.Millisecond)
			log.Printf("id[%d].state[%v].term[%d]: 重置选举计时器为[%d]ms\n", rf.me, rf.state, rf.currentTerm, timeout)
			rf.mu.Unlock()

		case <-rf.timerHeartBeat.C:
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			if rf.state == LEADER {
				//当心跳计时器到时间后,如果是Leader就开启心跳检测
				go rf.startBroadcast()
			}
			//重置心跳计时器
			rf.timerHeartBeat.Reset(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
			rf.mu.Unlock()
		}
	}

}

//发起投票
func (rf *Raft) startElection() {
	voteCh := make(chan bool, len(rf.peers))
	rf.mu.Lock()
	rf.toCandidate()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: -1,
			LastLogTerm:  -1,
		}
		if len(rf.log) > 0 {
			args.LastLogIndex = len(rf.log) - 1
			args.LastLogTerm = rf.log[args.LastLogIndex].Term
		}
		reply := &RequestVoteReply{}
		go func(i int, args *RequestVoteArgs, reply *RequestVoteReply) {
			rf.mu.Lock()
			log.Printf("id[%d].state[%v].term[%d]: 向 [%d] 申请选票\n", rf.me, rf.state, rf.currentTerm, i)
			rf.mu.Unlock()
			ok := rf.sendRequestVote(i, args, reply)
			if !ok {
				rf.mu.Lock()
				log.Printf("id[%d].state[%v].term[%d]: request vote to [%d] error\n", rf.me, rf.state, rf.currentTerm, i)
				rf.mu.Unlock()
				voteCh <- false
				return
			}
			rf.mu.Lock()
			//处理返回的term
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.toFollower()
				rf.voteFor = -1
			}
			rf.mu.Unlock()
			//log.Printf("id[%d].state[%v].term[%d]: 向 [%d] 申请选票的结果:%v\n", rf.me, rf.state, rf.currentTerm, i, reply.VoteGranted)
			voteCh <- reply.VoteGranted
		}(i, args, reply)
	}
	rf.mu.Unlock()
	i := 0
	t := 0
	for t < len(rf.peers)-1 {
		bool := <-voteCh
		t++
		if bool {
			i++
		}
		if i >= len(rf.peers)/2 {
			//超过半数成为leader
			rf.mu.Lock()
			if rf.state == CANDIDATE {
				go rf.toLeader()
			}
			rf.mu.Unlock()
			break
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]*LogEntry, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeoutHeartBeat = 150
	rf.timeoutElect = 150
	rf.timerHeartBeat = time.NewTimer(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
	rf.timerElect = time.NewTimer(time.Duration(rf.timeoutElect) * time.Millisecond)
	log.Printf("id[%d].state[%v].term[%d]: finish init\n", rf.me, rf.state, rf.currentTerm)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

//转变为leader
func (rf *Raft) toLeader() {
	rf.mu.Lock()
	log.Printf("id[%d].state[%v].term[%d]: 成为Leader\n", rf.me, rf.state, rf.currentTerm)
	rf.state = LEADER
	//1.初始化volatile state on leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	//初始化nextIndex为commitIndex+1
	for i, _ := range rf.nextIndex {
		rf.nextIndex[i] = rf.commitIndex + 1
	}
	//初始化matchIndex为0(实例化的时候已经赋值0了,不需要自己再赋值一次了)
	rf.mu.Unlock()
}

//转变为follower
func (rf *Raft) toFollower() {
	log.Printf("id[%d].state[%v].term[%d]: 变成Follower\n", rf.me, rf.state, rf.currentTerm)
	rf.state = FOLLOWER
}

//转变为候选人
func (rf *Raft) toCandidate() {
	//切换状态
	rf.state = CANDIDATE
	//自增任期号
	rf.currentTerm++
	//给自己投票
	rf.voteFor = rf.me
	log.Printf("id[%d].state[%v].term[%d]: 变成Candidate\n", rf.me, rf.state, rf.currentTerm)
}

func (rf *Raft) startBroadcast() {
	rf.mu.Lock()
	if rf.state == LEADER {
		log.Printf("id[%d].state[%v].term[%d]: 开始发送一轮心跳包\n", rf.me, rf.state, rf.currentTerm)
		rf.mu.Unlock()
		for i := range rf.peers {
			rf.mu.Lock()
			if i != rf.me {
				args := &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: -1,
					PrevLogTerm:  -1,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				if len(rf.log) > 0 {
					args.PrevLogIndex = len(rf.log) - 1
					args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				}
				//TODO:处理日志落后的server
				//当该跟随着的nextIndex小于等于leader的lastLogIndex时则发送索引
				if len(rf.log)-1 >= rf.nextIndex[i] {

				}
				reply := &AppendEntriesReply{}
				go rf.sendHeartBeat(i, args, reply)
			}
			rf.mu.Unlock()
		}
	} else {
		rf.mu.Unlock()
	}
}
func (rf *Raft) sendHeartBeat(i int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//log.Printf("id[%d].state[%v].term[%d]: 发送heartBeat to [%d]\n", rf.me, rf.state, rf.currentTerm, i)
	ok := rf.sendAppendEntries(i, args, reply)
	if !ok {
		//log.Printf("id[%d].state[%v].term[%d]: 发送heartBeat to [%d] error\n", rf.me, rf.state, rf.currentTerm, i)
		//rf.mu.Unlock()
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		//修改term
		rf.currentTerm = reply.Term
		//转变为follower
		rf.toFollower()
	}
	rf.mu.Unlock()
}

//处理不一致的日志
func (rf *Raft) dealLog() {

}
