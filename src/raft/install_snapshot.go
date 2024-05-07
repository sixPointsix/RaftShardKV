package raft

// 直接发，没有如figure13的分块
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// 处理快照
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	LOG_RAFT(Info, " #%d receive snapshot from #%d\n", rf.me, args.LeaderId)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	rf.ChangeState("follower")
	rf.electionTimer.Reset(getRandomTimeout())

	//收到过期的快照，还没自己的状态机新，就没必要更新了
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}

func (rf *Raft) getInstallSnapshotArgs(peer int) *InstallSnapshotArgs {
	args := new(InstallSnapshotArgs)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LastIncludedIndex = rf.log[0].Index
	args.LastIncludedTerm = rf.log[0].Term
	args.Data = rf.persister.ReadSnapshot()
	return args
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//失序的Snapshot
	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	if lastIncludedIndex > rf.log[len(rf.log)-1].Index {
		//完全包含
		rf.log = make([]Entry, 1)
	} else {
		//部分包含
		firstIndex := rf.log[0].Index
		tmpLog := make([]Entry, len(rf.log)-lastIncludedIndex+firstIndex)
		copy(tmpLog, rf.log[lastIncludedIndex-firstIndex:])
		rf.log = tmpLog
	}

	rf.log[0].Term = lastIncludedTerm
	rf.log[0].Index = lastIncludedIndex
	rf.log[0].Command = nil

	//因为是从上层传下来的快照，所以无需再提交
	rf.lastApplied, rf.commitIndex = lastIncludedIndex, lastIncludedIndex

	rf.persister.SaveStateAndSnapshot(rf.raftEncode(), snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == "leader" {
		LOG_RAFT(Debug, " leader Snapshot index: %d\n", index)
	}

	firstIndex := rf.log[0].Index
	if index <= firstIndex {
		//快照并没有更新
		return
	}

	//采用copy的方式及时回收用不到的rf.log内存
	tmpLog := make([]Entry, len(rf.log)-index+firstIndex)
	copy(tmpLog, rf.log[index-firstIndex:])
	rf.log = tmpLog

	//rf.log[0]保存的就是snapshot的最后元素的index和term
	rf.log[0].Command = nil

	//持久化
	rf.persister.SaveStateAndSnapshot(rf.raftEncode(), snapshot)
}

// InstallSnapshot call
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//Call 收到信息返回true，time-out收不到返回false
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok == true {
		return true
	} else {
		return false
	}
}
