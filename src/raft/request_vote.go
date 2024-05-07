package raft

import (
	"time"
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//没投过票且该follower拥有的日志至少与自己up-to-date
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.ChangeState("follower")
		rf.votedFor = -1
	}

	if !rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) {
		LOG_RAFT(Warn, " #%d 因为日志不匹配而拒绝投票\n", rf.me)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//只有投票了(VoteGranted)才能重置选举超时计时器
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(getRandomTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

// 判断是否up-to-date
func (rf *Raft) isUpToDate(lastLogTerm int, lastLogIndex int) bool {
	myLastLogTerm, myLastLogIndex := rf.log[len(rf.log)-1].Term, rf.log[len(rf.log)-1].Index

	if lastLogTerm > myLastLogTerm {
		return true
	}
	if lastLogTerm == myLastLogTerm {
		return lastLogIndex >= myLastLogIndex
	}

	return false
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	//Call 收到信息返回true，time-out收不到返回false
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok == true {
		return true
	} else {
		return false
	}
}

func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.ChangeState("candidate")
	rf.votedFor = rf.me
	rf.persist()

	tickets := 1
	args := rf.getRequestVoteArgs()
	for i := range rf.peers {
		//并行发送RequestVote RPC
		if i == rf.me {
			continue
		}
		go func(num int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(num, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				//只收仍处于本状态的PRCreply
				if rf.state == "candidate" && rf.currentTerm == args.Term {
					if reply.VoteGranted == true {
						LOG_RAFT(Info, " #%d get vote form #%d, %v\n", rf.me, num, reply)
						tickets++
						if tickets > len(rf.peers)/2 {
							//candidate->leader，并开始发送心跳
							rf.ChangeState("leader")
							LOG_RAFT(Important, " #%d be leader, Term: %d\n", rf.me, rf.currentTerm)
							rf.replicate(true)
							rf.heartbeatTimer.Reset(time.Millisecond * 100)
						}
					} else if rf.currentTerm < reply.Term {
						//candidate->follower
						rf.ChangeState("follower")
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.electionTimer.Reset(getRandomTimeout())

						rf.persist()
					}
				}
			}
		}(i)
	}
}

func (rf *Raft) getRequestVoteArgs() *RequestVoteArgs {
	args := new(RequestVoteArgs)
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.log[len(rf.log)-1].Index
	args.LastLogTerm = rf.log[len(rf.log)-1].Term
	return args
}
