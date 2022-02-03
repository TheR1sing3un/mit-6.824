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
	timerHeartBeat   *time.Timer   //心跳计时器
	timerElect       *time.Timer   //选举计时器
	timeoutHeartBeat int           //心跳频率/ms
	timeoutElect     int           //选举频率/ms
	applyCh          chan ApplyMsg //命令应用通道
	//最近快照的数据
	lastIncludedIndex int //最近快照的lastIncludedIndex
	lastIncludedTerm  int //最近快照的lastIncludedTerm
}

//日志条目
type LogEntry struct {
	Command interface{} //日志记录的命令(用于应用服务的命令)
	Term    int         //该日志被接收的时候的Leader任期
	Index   int         //该日志的索引
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
	//编码lastIncludedIndex
	err = e.Encode(rf.lastIncludedIndex)
	if err != nil {
		log.Printf("id[%d].state[%v].term[%d]: encode lastIncludedIndex error: %v\n", rf.me, rf.state, rf.currentTerm, err)
	}
	//编码lastIncludedTerm
	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		log.Printf("id[%d].state[%v].term[%d]: encode lastIncludedTerm error: %v\n", rf.me, rf.state, rf.currentTerm, err)
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logEntries) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil {
		log.Printf("id[%d].state[%v].term[%d]: decode error\n", rf.me, rf.state, rf.currentTerm)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logEntries = logEntries
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
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

//快照安装RPC的参数
type InstallSnapshotArgs struct {
	Term              int    //leader的任期
	LeaderId          int    //leader的id
	LastIncludedIndex int    //快照中包含的最后一个日志条目的index
	LastIncludedTerm  int    //快照中包含的最后一个日志条目的term
	Data              []byte //快照数据
}

//快照安装的返回值
type InstallSnapshotReply struct {
	Term int //接收者的currentTerm
}

// InstallSnapshot 快照安装的RPC
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()

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
	defer rf.persist()
	log.Printf("id[%d].state[%v].term[%d]: 接收到[%d]的选举申请\n", rf.me, rf.state, rf.currentTerm, args.CandidateId)
	defer func() {
		log.Printf("id[%d].state[%v].term[%d]: 给[%d]的选举申请返回%v\n", rf.me, rf.state, rf.currentTerm, args.CandidateId, reply.VoteGranted)
	}()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	reply.VoteGranted = false
	//1.如果Term<currentTerm或者已经投过票了,则之直接返回拒绝
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		return
	}
	//2.如果t > currentTerm,则更新currentTerm,并切换为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.toFollower()
		rf.voteFor = -1
	}
	////3.判断候选人的日志是否最少一样新
	////如果两份日志最后的条目的任期号不同,那么任期号大的日志更加新;如果两份日志最后的条目任期号相同,那么日志比较长的那个就更加新
	//if len(rf.logEntries)-1 == 0 || args.LastLogTerm > rf.logEntries[len(rf.logEntries)-1].Term || (args.LastLogTerm == rf.logEntries[len(rf.logEntries)-1].Term && args.LastLogIndex >= len(rf.logEntries)-1) {
	//	//重置选举时间
	//	rf.resetElectTimer()
	//	//投票给候选人
	//	rf.voteFor = args.CandidateId
	//	//投赞成
	//	reply.VoteGranted = true
	//}

	//3.判断候选人的日志是否最少一样新(2D版本)
	//如果两份日志最后的条目的任期号不同,那么任期号大的日志更加新;如果两份日志最后的条目任期号相同,那么日志比较长的那个就更加新
	if rf.lastLogTerm() == -1 || args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex()) {
		//重置选举时间
		rf.resetElectTimer()
		//投票给候选人
		rf.voteFor = args.CandidateId
		//投赞成
		reply.VoteGranted = true
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
	XTerm   int  //若follower和leader的日志冲突,则记载的是follower的log在preLogIndex处的term,若preLogIndex处无日志,返回-1
	XIndex  int  //follower中的log里term为XTerm的第一条log的index
	XLen    int  //当XTerm为-1时,此时XLen记录follower的日志长度(不包含初始占位日志)
}

//日志追加的RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//将自己的term返回
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
	//若请求的term大于该server的term,则更新term并且将voteFor置为未投票
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	//重置选举时间
	rf.resetElectTimer()
	//转变为follower
	rf.toFollower()
	//进行日志一致性判断(快速恢复)
	//若leader在preLogIndex处没有日志
	if rf.lastLogIndex() < args.PrevLogIndex {
		reply.Term = 0
		reply.Success = false
		//preLogIndex处无日志,记录XTerm为-1
		reply.XTerm = -1
		//记录XLen为日志的长度(不包含初始占位日志)
		reply.XLen = len(rf.logEntries) - 1
		log.Printf("id[%d].state[%v].term[%d]: 追加日志的和现在的日志不匹配\n", rf.me, rf.state, rf.currentTerm)
		return
	}
	//若preLogIndex处的日志的term和preLogTerm不相等
	if rf.logEntries[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Success = false
		//更新XTerm为冲突的Term
		reply.XTerm = rf.logEntries[args.PrevLogIndex-rf.lastIncludedIndex].Term
		//更新XIndex为XTerm在本机log中第一个Index位置
		reply.XIndex = rf.binaryFindFirstIndexByTerm(reply.XTerm)
		log.Printf("id[%d].state[%v].term[%d]: 追加日志的和现在的日志不匹配\n", rf.me, rf.state, rf.currentTerm)
		return
	}
	//追加
	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > rf.lastLogIndex() {
			rf.logEntries = append(rf.logEntries, logEntry)
		} else { // 重叠部分
			if rf.logEntries[index-rf.lastIncludedIndex].Term != logEntry.Term {
				rf.logEntries = rf.logEntries[:index-rf.lastIncludedIndex] // 删除当前以及后续所有log
				rf.logEntries = append(rf.logEntries, logEntry)            // 把新log加入进来
			} // term一样啥也不用做，继续向后比对Log
		}
	}
	//rf.logEntries = append(rf.logEntries[:args.PrevLogIndex+1], args.Entries...)
	//更新follower的commitIndex
	rf.updateCommitIndexForFollower(args.LeaderCommit)
}

//最近的一个log的index
func (rf *Raft) lastLogIndex() int {
	////若无最新日志
	//	//if len(rf.logEntries) == 1 {
	//	//	//若有最近快照的数据
	//	//	if rf.lastIncludedIndex != 0 {
	//	//		return rf.lastIncludedIndex
	//	//	}
	//	//	//若无最近快照
	//	//	return rf.logEntries[0].Index
	//	//}
	//	////若有最近日志
	return rf.logEntries[len(rf.logEntries)-1].Index
}

//最近一个log的term
func (rf *Raft) lastLogTerm() int {
	////若无最新日志
	//if len(rf.logEntries) == 1 {
	//	//若有最近快照的数据
	//	if rf.lastIncludedTerm != 0 {
	//		return rf.lastIncludedTerm
	//	}
	//	//若无最近快照
	//	return rf.logEntries[0].Term
	//}
	//若有最近日志
	return rf.logEntries[len(rf.logEntries)-1].Term
}

//更新follower的commitIndex
func (rf *Raft) updateCommitIndexForFollower(leaderCommit int) {
	//如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引(leaderCommit > commitIndex)
	//则把接收者的已知已经提交的最高的日志条目的索引commitIndex
	//重置为 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	if leaderCommit > rf.commitIndex {
		if leaderCommit > rf.lastLogIndex() {
			rf.commitIndex = rf.lastLogIndex()
			log.Printf("id[%d].state[%v].term[%d]: 重置commitIndex为上一条新条目的索引[%d]\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex)
		} else {
			rf.commitIndex = leaderCommit
			log.Printf("id[%d].state[%v].term[%d]: 重置commitIndex为leaderCommit[%d]\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex)
		}
	}
}

//二分查找目标term的第一个log的index(寻找左边界)
func (rf *Raft) binaryFindFirstIndexByTerm(term int) int {
	left := 0
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
	//检查越界和没有找到的情况(这时候没找到代表是lastIncluded)
	if left >= len(rf.logEntries) || rf.logEntries[left].Term != term {
		return -1
	}
	return rf.logEntries[left].Index
}

//重置选举计时器
func (rf *Raft) resetElectTimer() {
	timeout := rand.Intn(400) + rf.timeoutElect
	rf.timerElect.Reset(time.Duration(timeout) * time.Millisecond)
	log.Printf("id[%d].state[%v].term[%d]: 重置选举计时器为[%d]ms\n", rf.me, rf.state, rf.currentTerm, timeout)
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
	//defer rf.mu.Unlock()
	if rf.state == LEADER {
		//若为leader则开始
		//构造日志
		log.Printf("id[%d].state[%v].term[%d]: 接收到命令command = %v\n", rf.me, rf.state, rf.currentTerm, command)
		//获取该日志在将存在本地的索引
		index = rf.lastLogIndex() + 1
		logEntry := LogEntry{
			Command: command,
			Term:    rf.currentTerm,
			Index:   index,
		}
		//存入本地
		rf.logEntries = append(rf.logEntries, logEntry)
		log.Printf("id[%d].state[%v].term[%d]: 保存命令command = %v到本地log\n", rf.me, rf.state, rf.currentTerm, command)
		rf.persist()
		//获取该日志的任期
		term = rf.logEntries[index-rf.lastIncludedIndex].Term
		rf.mu.Unlock()
		//立马进行一次广播
		go rf.BoardCast()
		return index, term, isLeader
	}
	rf.mu.Unlock()
	isLeader = false
	return index, term, isLeader
}

//检查更新commitIndex
func (rf *Raft) UpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.state == LEADER {
		//从lastLog开始
		for i := rf.lastLogIndex(); i > rf.commitIndex; i-- {
			updateConNum := len(rf.peers) / 2
			num := 0
			for j := range rf.peers {
				if j == rf.me {
					continue
				}
				//若match[j] >= i 而且log[i].Term == currentTerm则该server符合更新要求
				if rf.matchIndex[j] >= i && rf.logEntries[i-rf.lastIncludedIndex].Term == rf.currentTerm {
					num++
				}
			}
			//若过半数则更新commitIndex
			if num >= updateConNum {
				rf.commitIndex = i
				log.Printf("id[%d].state[%v].term[%d]: n = %d, 过半节点的matchIndex >= n而且log[n].Term == currentTerm,则更新commitIndex = %d\n", rf.me, rf.state, rf.currentTerm, i, i)
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(1 * time.Millisecond)
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
				go rf.StartElection()
			}
			//重置选举计时器
			//timeout := rand.Intn(150) + rf.timeoutElect
			//rf.timerElect.Reset(time.Duration(timeout) * time.Millisecond)
			//log.Printf("id[%d].state[%v].term[%d]: 重置选举计时器为[%d]ms\n", rf.me, rf.state, rf.currentTerm, timeout)
			rf.resetElectTimer()
			rf.mu.Unlock()

		case <-rf.timerHeartBeat.C:
			if rf.killed() {
				break
			}
			rf.mu.Lock()
			if rf.state == LEADER {
				//当心跳计时器到时间后,如果是Leader就开启心跳检测
				go rf.BoardCast()
				//go rf.SendLog()
			}
			//重置心跳计时器
			rf.timerHeartBeat.Reset(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
			rf.mu.Unlock()
		}
	}

}

// StartElection 发起选举
func (rf *Raft) StartElection() {
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
			LastLogIndex: rf.lastLogIndex(),
			LastLogTerm:  rf.lastLogTerm(),
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
				go rf.ToLeader()
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
	rf.logEntries = append(rf.logEntries, LogEntry{-1, -1, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = FOLLOWER
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.timeoutHeartBeat = 100
	rf.timeoutElect = 600
	rf.timerHeartBeat = time.NewTimer(time.Duration(rf.timeoutHeartBeat) * time.Millisecond)
	rf.timerElect = time.NewTimer(time.Duration(rf.timeoutElect) * time.Millisecond)
	rf.applyCh = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1
	log.Printf("id[%d].state[%v].term[%d]: finish init\n", rf.me, rf.state, rf.currentTerm)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.ApplyCommand()
	return rf
}

// ApplyCommand 检查是否 commitIndex > lastApplied,若是则lastApplied递增,并将log[lastApplied]应用到状态机
func (rf *Raft) ApplyCommand() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		//log.Printf("id[%d].state[%v].term[%d]:commitIndex[%d];lastIncludedIndex[%d]\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastIncludedIndex)
		if rf.logEntries[rf.commitIndex-rf.lastIncludedIndex].Term != rf.currentTerm {
			rf.mu.Unlock()
			continue
		}
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				Command:      rf.logEntries[rf.lastApplied-rf.lastIncludedIndex].Command,
				CommandIndex: rf.lastApplied,
				CommandValid: true,
			}
			log.Printf("id[%d].state[%v].term[%d]: 检测到commitIndex[%d] > lastApplied[%d],更新lastApplied = %d,并应用到状态机中\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied-1, rf.lastApplied)
			rf.applyCh <- applyMsg
		}
		rf.mu.Unlock()
	}
}

// ToLeader 转变为leader
func (rf *Raft) ToLeader() {
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
	go rf.UpdateCommitIndex()
	//立马开始一轮心跳
	rf.timerHeartBeat.Reset(0)
}

//转变为follower
func (rf *Raft) toFollower() {
	if rf.state == FOLLOWER {
		return
	}
	rf.state = FOLLOWER
	log.Printf("id[%d].state[%v].term[%d]: 变成Follower\n", rf.me, rf.state, rf.currentTerm)
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

// BoardCast 发起广播发送AppendEntries RPC
func (rf *Raft) BoardCast() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		log.Printf("id[%d].state[%v].term[%d]: 开始发送一轮AE发送\n", rf.me, rf.state, rf.currentTerm)
		for i := range rf.peers {
			if i != rf.me {
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logEntries[rf.nextIndex[i]-1-rf.lastIncludedIndex].Term,
					Entries:      rf.logEntries[rf.nextIndex[i]-rf.lastIncludedIndex:],
					LeaderCommit: rf.commitIndex,
				}
				reply := AppendEntriesReply{}
				log.Printf("id[%d].state[%v].term[%d]: server[%d]的nextIndex=[%d],matchIndex=[%d]\n", rf.me, rf.state, rf.currentTerm, i, rf.nextIndex[i], rf.matchIndex[i])
				go rf.HandleAppendEntries(i, args, reply)
			}
		}
	}
}

func (rf *Raft) HandleAppendEntries(server int, args AppendEntriesArgs, reply AppendEntriesReply) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(server, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	log.Printf("id[%d].state[%v].term[%d]: 此时已有的log=[%v]\n", rf.me, rf.state, rf.currentTerm, rf.logEntries)
	//过期的请求直接结束
	if rf.state != LEADER || args.Term != rf.currentTerm {
		return
	}
	if !ok {
		log.Printf("id[%d].state[%v].term[%d]: 发送ae to [%d] error\n", rf.me, rf.state, rf.currentTerm, server)
		return
	}
	//若返回失败
	if !reply.Success {
		//若因为任期不匹配导致失败
		if reply.Term > rf.currentTerm {
			//修改term
			rf.currentTerm = reply.Term
			//转变为follower
			rf.toFollower()
			log.Printf("id[%d].state[%v].term[%d]: 发送ae to [%d] 过期,转变为follower\n", rf.me, rf.state, rf.currentTerm, server)
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
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		log.Printf("id[%d].state[%v].term[%d]: 追加日志到server[%d]成功,更新nextIndex->[%d],matchIndex->[%d]\n", rf.me, rf.state, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])
	}
}
