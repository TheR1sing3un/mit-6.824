package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logEntries entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logEntries, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
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
// as each Raft peer becomes aware that successive logEntries entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logEntries entry.
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
	currentTerm int        //当前任期
	voteFor     int        //当前任期投给的候选人id(为-1时代表没有投票)
	logEntries  []LogEntry //日志条目
	//volatile state on all servers
	state       RaftState //当前raft状态
	commitIndex int       //当前log中的最高索引(从0开始,递增)
	lastApplied int       //当前被用到状态机中的日志最高索引(从0开始,递增)
	//volatile state on leader
	nextIndex  []int //发送给每台服务器的下一条日志目录索引(初始值为leader的commitIndex + 1)
	matchIndex []int //每台服务器已知的已被复制的最高日志条目索引

	//自定义参数
	timerHeartBeat   *time.Timer    //心跳计时器
	timerElect       *time.Timer    //选举计时器
	timeoutHeartBeat int            //心跳频率/ms
	timeoutElect     int            //选举频率/ms
	applyCh          *chan ApplyMsg //命令应用通道
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码currentTerm
	err := e.Encode(rf.currentTerm)
	if err != nil {
		log.Printf("id[%d].state[%v].term[%d]: encode currentTerm error: %v\n", rf.me, rf.state, rf.currentTerm, err)
		return
	}
	//编码voteFor
	err = e.Encode(rf.voteFor)
	if err != nil {
		log.Printf("id[%d].state[%v].term[%d]: encode voteFor error: %v\n", rf.me, rf.state, rf.currentTerm, err)
		return
	}
	//编码log[]
	err = e.Encode(rf.logEntries)
	if err != nil {
		log.Printf("id[%d].state[%v].term[%d]: encode logEntries[] error: %v\n", rf.me, rf.state, rf.currentTerm, err)
	}
	data := w.Bytes()
	//保存持久化状态
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logEntries) != nil {
		log.Printf("id[%d].state[%v].term[%d]: decode error\n", rf.me, rf.state, rf.currentTerm)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logEntries = logEntries
	}
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
// service no longer needs the logEntries through (and including)
// that index. Raft should now trim its logEntries as much as possible.
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
	reply.VoteGranted = false
	//1.如果t > currentTerm,则更新currentTerm,并切换为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.state != LEADER {
			rf.toFollower()
		}
		rf.voteFor = -1
		rf.persist()
	}
	//2.如果Term<currentTerm或者已经投过票了,则之直接返回拒绝
	if args.Term < rf.currentTerm || rf.voteFor != -1 {
		return
	}
	//3.判断候选人的日志是否最少一样新
	//如果两份日志最后的条目的任期号不同,那么任期号大的日志更加新;如果两份日志最后的条目任期号相同,那么日志比较长的那个就更加新
	if len(rf.logEntries)-1 == 0 || args.LastLogTerm > rf.logEntries[len(rf.logEntries)-1].Term || (args.LastLogTerm == rf.logEntries[len(rf.logEntries)-1].Term && args.LastLogIndex >= len(rf.logEntries)-1) {
		//重置选举时间
		timeout := rand.Intn(300) + rf.timeoutElect
		rf.timerElect.Reset(time.Duration(timeout) * time.Millisecond)
		log.Printf("id[%d].state[%v].term[%d]: 重置选举计时器为[%d]ms\n", rf.me, rf.state, rf.currentTerm, timeout)
		//投票给候选人
		rf.voteFor = args.CandidateId
		//更新状态为follower
		rf.toFollower()
		//投赞成
		reply.VoteGranted = true
	}
	rf.persist()
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
	XTerm   int  //若follower和leader的日志冲突,则记载的是follower的log在preLogIndex处的term,若preLogIndex处无日志,返回-1
	XIndex  int  //follower中的log里term为XTerm的第一条log的index
	XLen    int  //当XTerm为-1时,此时XLen记录follower的日志长度(不包含初始占位日志)
}

//日志追加的RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	reply.Success = true
	log.Printf("id[%d].state[%v].term[%d]: 接收到[%d],term[%d]的日志追加,preLogIndex = [%d], preLogTerm = [%d],entries = [%v]\n", rf.me, rf.state, rf.currentTerm, args.LeaderId, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.Entries)
	log.Printf("id[%d].state[%v].term[%d]: 此时已有的log=[%v]\n", rf.me, rf.state, rf.currentTerm, rf.logEntries)
	//判断term是否小于当前任期
	if args.Term < rf.currentTerm {
		log.Printf("id[%d].state[%v].term[%d]: 追加日志的任期%d小于当前任期%d\n", rf.me, rf.state, rf.currentTerm, args.Term, rf.currentTerm)
		reply.Success = false
		return
	}
	//重置选举时间
	timeout := rand.Intn(300) + rf.timeoutElect
	rf.timerElect.Reset(time.Duration(timeout) * time.Millisecond)
	log.Printf("id[%d].state[%v].term[%d]: 重置选举计时器为[%d]ms\n", rf.me, rf.state, rf.currentTerm, timeout)
	//更新currentTerm
	rf.currentTerm = args.Term
	rf.persist()
	//转变为follower
	if rf.state != FOLLOWER {
		rf.toFollower()
	}
	//进行日志一致性判断
	//若leader的日志不为空而且此时follower中该prevLogIndex处的日志的term和leader的prevLogTerm不等则日志不匹配
	//if len(rf.logEntries)-1 < args.PrevLogIndex || args.PrevLogTerm != rf.logEntries[args.PrevLogIndex].Term {
	//	logEntries.Printf("id[%d].state[%v].term[%d]: 追加日志的和现在的日志不匹配\n", rf.me, rf.state, rf.currentTerm)
	//	reply.Success = false
	//	return
	//}

	//进行日志一致性判断(快速恢复)
	//若leader在preLogIndex处没有日志
	if len(rf.logEntries)-1 < args.PrevLogIndex {
		reply.Success = false
		//preLogIndex处无日志,记录XTerm为-1
		reply.XTerm = -1
		//记录XLen为日志的长度(不包含初始占位日志)
		reply.XLen = len(rf.logEntries) - 1
		log.Printf("id[%d].state[%v].term[%d]: 追加日志的和现在的日志不匹配\n", rf.me, rf.state, rf.currentTerm)
		return
	}
	//若preLogIndex处的日志的term和preLogTerm不相等
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		//更新XTerm为冲突的Term
		reply.XTerm = rf.logEntries[args.PrevLogIndex].Term
		//更新XIndex为XTerm在本机log中第一个Index位置
		reply.XIndex = rf.binaryFindFirstIndexByTerm(reply.XTerm)
		log.Printf("id[%d].state[%v].term[%d]: 追加日志的和现在的日志不匹配\n", rf.me, rf.state, rf.currentTerm)
		return
	}

	//开始日志同步
	//如果一个已经存在的条目和新条目(即刚刚接收到的日志条目)发生了冲突(因为索引相同，任期不同),那么就删除这个已经存在的条目以及它之后的所有条目
	for i := 0; i < len(args.Entries); i++ {
		if len(rf.logEntries)-1 > args.PrevLogIndex+i && rf.logEntries[args.PrevLogIndex+1+i].Term != args.Entries[i].Term {
			rf.logEntries = rf.logEntries[:args.PrevLogIndex+1+i]
			log.Printf("id[%d].state[%v].term[%d]: index[%d]处日志的Term新日志entries[%d]的Term[%d]不匹配,删除index[%d]及其以后的log\n",
				rf.me, rf.state, rf.currentTerm, args.PrevLogIndex+1+i, i, args.Entries[i].Term, args.PrevLogIndex+1+i)
		}
	}
	//追加日志中尚未存在的任何新条目
	for i, entry := range args.Entries {
		if args.PrevLogIndex+1+i > len(rf.logEntries)-1 {
			rf.logEntries = append(rf.logEntries, entry)
			log.Printf("id[%d].state[%v].term[%d]: 保存日志[%v]到本队log中\n", rf.me, rf.state, rf.currentTerm, entry)
		}
	}
	//rf.logEntries = append(rf.logEntries[:args.PrevLogIndex+1], args.Entries...)
	//如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引(leaderCommit > commitIndex)
	//则把接收者的已知已经提交的最高的日志条目的索引commitIndex
	//重置为 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.logEntries)-1 {
			rf.commitIndex = len(rf.logEntries) - 1
			log.Printf("id[%d].state[%v].term[%d]: 重置commitIndex为上一条新条目的索引[%d]\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex)

		} else {
			rf.commitIndex = args.LeaderCommit
			log.Printf("id[%d].state[%v].term[%d]: 重置commitIndex为leaderCommit[%d]\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex)
		}
	}
	rf.persist()
}

//二分查找目标term的第一个log的index(寻找左边界)
func (rf *Raft) binaryFindFirstIndexByTerm(term int) int {
	left := 1
	right := len(rf.logEntries) - 1
	for left <= right {
		mid := left + (right-left)/2
		if rf.logEntries[mid].Term < term {
			left = mid + 1
		} else if rf.logEntries[mid].Term > term {
			right = mid - 1
		} else {
			//为了寻找左边界,这里仍然将移动右指针
			right = mid - 1
		}
	}
	//检查越界和没有找到的情况
	if left >= len(rf.logEntries) || rf.logEntries[left].Term != term {
		return -1
	}
	return left
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
// agreement on the next command to be appended to Raft's logEntries. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logEntries, since the leader
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
	rf.mu.Lock()
	if rf.state == LEADER {
		//若为leader则开始
		//构造日志
		log.Printf("id[%d].state[%v].term[%d]: 接收到命令command = %v\n", rf.me, rf.state, rf.currentTerm, command)
		logEntry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
		}
		//获取该日志在将存在本地的索引
		index = len(rf.logEntries)
		//存入本地
		rf.logEntries = append(rf.logEntries, logEntry)
		log.Printf("id[%d].state[%v].term[%d]: 保存命令command = %v到本地log\n", rf.me, rf.state, rf.currentTerm, command)
		rf.persist()
		//获取该日志的任期
		term = rf.logEntries[index].Term
		rf.mu.Unlock()
		//重置心跳时间(立马发送心跳检测,更新日志)
		rf.timerHeartBeat.Reset(0)
		return index, term, isLeader
	}
	rf.mu.Unlock()
	isLeader = false
	return index, term, isLeader
}

//检查更新commitIndex
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.state == LEADER {
		//n从commitIndex+1开始
		n := rf.commitIndex + 1
		//若达到该数目成立,则更新(不算本节点)
		updateConNum := len(rf.peers) / 2
		num := 0
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			//若matchIndex[i] >= n 而且log[n].term == currentTerm则该点成立
			if len(rf.logEntries)-1 >= n && rf.matchIndex[i] >= n {
				num++
			}
		}
		//若过半数
		if num >= updateConNum {
			//更新commitIndex
			rf.commitIndex = n
			log.Printf("id[%d].state[%v].term[%d]: n = %d, 过半节点的matchIndex >= n而且log[n].Term == currentTerm,则更新commitIndex = %d\n", rf.me, rf.state, rf.currentTerm, n, n)
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
	}
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
// heartbeats recently.
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
			timeout := rand.Intn(300) + rf.timeoutElect
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
				go rf.boardCast()
				//go rf.SendLog()
			}
			//重置心跳计时器
			rf.timerHeartBeat.Reset(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
			rf.mu.Unlock()
		}
	}

}

//发起选举
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
			LastLogIndex: len(rf.logEntries) - 1,
			LastLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
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
				rf.persist()
			}
			rf.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(i, args, reply)
	}
	rf.mu.Unlock()
	i := 0
	t := 0
	for t < len(rf.peers)-1 {
		voteOk := <-voteCh
		t++
		if voteOk {
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
	rf.logEntries = make([]LogEntry, 0)
	rf.logEntries = append(rf.logEntries, LogEntry{-1, -1})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeoutHeartBeat = 100
	rf.timeoutElect = 600
	rf.timerHeartBeat = time.NewTimer(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
	rf.timerElect = time.NewTimer(time.Duration(rf.timeoutElect) * time.Millisecond)
	rf.applyCh = &applyCh
	//rf.persist()
	log.Printf("id[%d].state[%v].term[%d]: finish init\n", rf.me, rf.state, rf.currentTerm)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommand()
	return rf
}

//检查是否 commitIndex > lastApplied,若是则lastApplied递增,并将log[lastApplied]应用到状态机
func (rf *Raft) applyCommand() {
	for !rf.killed() {
		time.Sleep(5 * time.Millisecond)
		rf.mu.Lock()
		if rf.logEntries[rf.commitIndex].Term != rf.currentTerm {
			rf.mu.Unlock()
			continue
		}
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := &ApplyMsg{
				Command:      rf.logEntries[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
				CommandValid: true,
			}
			log.Printf("id[%d].state[%v].term[%d]: 检测到commitIndex[%d] > lastApplied[%d],更新lastApplied = %d,并应用到状态机中\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied-1, rf.lastApplied)
			*rf.applyCh <- *applyMsg
		}
		rf.mu.Unlock()
	}
}

//转变为leader
func (rf *Raft) toLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	//当为leader时,开始启动协程来实时更新commitIndex
	go rf.updateCommitIndex()
	//立马开始一轮心跳
	rf.timerHeartBeat.Reset(0)
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

//发起广播发送AppendEntries RPC
func (rf *Raft) boardCast() {
	rf.mu.Lock()
	if rf.state == LEADER {
		log.Printf("id[%d].state[%v].term[%d]: 开始发送一轮AE发送\n", rf.me, rf.state, rf.currentTerm)
		rf.mu.Unlock()
		for i := range rf.peers {
			rf.mu.Lock()
			if i != rf.me {
				go rf.handleAppendEntries(i)
			}
			rf.mu.Unlock()
		}
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) handleAppendEntries(server int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.nextIndex[server] - 1,
		PrevLogTerm:  rf.logEntries[rf.nextIndex[server]-1].Term,
		Entries:      rf.logEntries[rf.nextIndex[server]:],
		LeaderCommit: rf.commitIndex,
	}
	log.Printf("id[%d].state[%v].term[%d]: server[%d]的nextIndex=[%d],matchIndex=[%d]\n", rf.me, rf.state, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])
	reply := AppendEntriesReply{}
	targetNextIndex := rf.nextIndex[server] + len(args.Entries)
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("id[%d].state[%v].term[%d]: 此时已有的log=[%v]\n", rf.me, rf.state, rf.currentTerm, rf.logEntries)
	if !ok {
		log.Printf("id[%d].state[%v].term[%d]: 发送ae to [%d] error\n", rf.me, rf.state, rf.currentTerm, server)
		return
	}
	//若返回失败
	if !reply.Success {
		if reply.Term > rf.currentTerm {
			//修改term
			rf.currentTerm = reply.Term
			//转变为follower
			rf.toFollower()
			log.Printf("id[%d].state[%v].term[%d]: 发送ae to [%d] 过期,转变为follower\n", rf.me, rf.state, rf.currentTerm, server)
			rf.persist()
			return
		}
		//若不是因为任期拒绝则是因为日志不匹配
		//更新nextIndex
		if len(args.Entries) > 0 {
			//当follower的preLogIndex处无日志时
			if reply.XTerm == -1 {
				//更新nextIndex为follower的最后一条日志的下一个位置
				rf.nextIndex[server] = reply.XLen + 1
			} else {
				//当preLogIndex处的日志任期冲突时
				//更新nextIndex为该冲突任期的第一条日志的位置,为了直接覆盖冲突的任期的所有的日志
				rf.nextIndex[server] = reply.XIndex
			}
			log.Printf("id[%d].state[%v].term[%d]: 追加日志到server[%d]失败,更新nextIndex->[%d],matchIndex->[%d]\n", rf.me, rf.state, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])
		}
		return
	}

	if len(args.Entries) > 0 {
		if targetNextIndex > rf.nextIndex[server] {
			rf.nextIndex[server] = targetNextIndex
			rf.matchIndex[server] = targetNextIndex - 1
			log.Printf("id[%d].state[%v].term[%d]: 追加日志到server[%d]成功,更新nextIndex->[%d],matchIndex->[%d]\n", rf.me, rf.state, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])
		}
	}
}
