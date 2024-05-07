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

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"math/rand"

	"gp/labgob"

	"gp/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	state          string              //状态：leader, follower, candidate
	electionTimer  *time.Timer         //选举超时计时器(300-600ms)
	heartbeatTimer *time.Timer         //心跳计时器(150ms)
	applyCond      *sync.Cond          //applier的条件变量
	replicatorCond []*sync.Cond        //replicator的条件变量
	applyCh        chan ApplyMsg       //applier管道
	// Your data here (2A, 2B, 2C).
	currentTerm int
	votedFor    int
	log         []Entry
	commitIndex int //Leader已经提交的
	lastApplied int //自己已经apply的
	//for Leader
	nextIndex  []int //对于每个server,下一个应发送的idx，从当前leader的logIndex+1开始
	matchIndex []int //每个每个server,已经复制了的最大idx，从0开始
}

type Entry struct {
	Command interface{}
	Term    int
	Index   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	if rf.state == "leader" {
		isleader = true
	} else {
		isleader = false
	}

	return term, isleader
}

func (rf *Raft) ChangeState(state string) {
	rf.state = state

	switch state {
	case "follower":
	case "candidate":
	case "leader":
		leaderLastLog := rf.log[len(rf.log)-1].Index
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = leaderLastLog + 1
			rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = leaderLastLog
		rf.electionTimer.Stop()
	}
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) raftEncode() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()

	return data
}

func (rf *Raft) persist() {
	data := rf.raftEncode()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []Entry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		LOG_RAFT(Error, " Read Persist failed!!!!!!!!!!!!!!!!!!!!!!!!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := len(rf.log) - 1; i > 0; i-- {
		if rf.log[i].Term > rf.currentTerm {
			continue
		}
		if rf.log[i].Term == rf.currentTerm {
			return true
		}
		if rf.log[i].Term < rf.currentTerm {
			break
		}
	}
	return false
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		return -1, -1, false
	}
	index = len(rf.log) + rf.log[0].Index
	term = rf.currentTerm

	//新增日志
	entry := Entry{command, term, index}
	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.log[0].Index
	rf.nextIndex[rf.me] = len(rf.log) - 1 + rf.log[0].Index

	rf.persist()
	//开始复制一轮
	rf.replicate(false)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
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
		select {
		case <-rf.electionTimer.C:
			LOG_RAFT(Debug, " #%d ElectionTimerTick\n", rf.me)
			rf.mu.Lock()
			rf.electionTimer.Reset(getRandomTimeout()) //每次投票之后重置计时器
			rf.startElection()
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == "leader" {
				rf.replicate(true) //心跳
				rf.heartbeatTimer.Reset(time.Millisecond * 100)
			}
			rf.mu.Unlock()
		}
	}
}

// 在300-600ms随机time-out
func getRandomTimeout() time.Duration {
	time_out := time.Millisecond * time.Duration(rand.Int63()%300+300)
	return time_out
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		if rf.lastApplied < rf.log[0].Index {
			//说明应该应用快照
			index := rf.log[0].Index
			term := rf.log[0].Term
			data := rf.persister.ReadSnapshot()
			rf.mu.Unlock()
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				SnapshotIndex: index,
				SnapshotTerm:  term,
				Snapshot:      data,
			}

			rf.mu.Lock()
			rf.lastApplied = rf.log[0].Index
			rf.mu.Unlock()
			continue
		}

		LOG_RAFT(Debug, " #%d apply log[%d,%d]\n", rf.me, rf.lastApplied+1, rf.commitIndex)
		//按序提交[lastApplied+1, commitIndex]之间的所有log
		LOG_RAFT(Debug, " #%d lastApplied:%d, commitIndex:%d\n", rf.me, rf.lastApplied, rf.commitIndex)
		firstIndex := rf.log[0].Index
		entries := make([]Entry, rf.commitIndex-rf.lastApplied)
		copy(entries, rf.log[rf.lastApplied+1-firstIndex:rf.commitIndex+1-firstIndex])
		commitIndex := rf.commitIndex
		rf.mu.Unlock()

		//此处是单协程处理applyCh，不涉及静态条件
		for _, entry := range entries {
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = entry.Command
			applyMsg.CommandIndex = entry.Index
			applyMsg.CommandTerm = entry.Term

			rf.applyCh <- applyMsg
		}

		//等写入完成后才能更新lastApplied
		rf.mu.Lock()
		rf.lastApplied = Max(rf.lastApplied, commitIndex)
		rf.persist()
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:          peers,
		persister:      persister,
		me:             me,
		dead:           0,
		state:          "follower",
		currentTerm:    0,
		votedFor:       -1,
		log:            make([]Entry, 1), //log索引从1开始
		commitIndex:    0,
		lastApplied:    0,
		applyCh:        applyCh,
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		replicatorCond: make([]*sync.Cond, len(peers)),
	}
	rf.readPersist(persister.ReadRaftState())
	/*if rf.lastApplied < rf.log[0].Index {
		lastIncludedIndex, lastIncludedTerm := rf.log[0].Index, rf.log[0].Term
		//加载快照
		rf.CondInstallSnapshot(lastIncludedTerm, lastIncludedIndex, rf.persister.ReadSnapshot())
	}*/
	rand.Seed(time.Now().Unix())

	rf.electionTimer = time.NewTimer(getRandomTimeout())
	rf.heartbeatTimer = time.NewTimer(time.Millisecond * 100)
	rf.applyCond = sync.NewCond(&rf.mu)

	for i := 0; i < len(peers); i++ {
		if i != me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			//采用后台协程+条件变量驱动的方式减少RPC的浪费
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.applier()
	go rf.ticker()

	return rf
}
