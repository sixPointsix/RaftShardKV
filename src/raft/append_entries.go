package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogTerm  int
	PrevLogIndex int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	LOG_RAFT(Debug, " #%d receive AE %v from #%d\n", rf.me, args, args.LeaderId)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.ChangeState("follower")
	rf.electionTimer.Reset(getRandomTimeout())

	//收到快照之前的日志条目
	if args.PrevLogIndex < rf.log[0].Index {
		reply.Term = 0
		reply.Success = false
		return
	}

	//Raft的一致性检查不匹配的情况
	firstIndex := rf.log[0].Index
	if len(rf.log)+firstIndex-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex-firstIndex].Term != args.PrevLogTerm {
		LOG_RAFT(Warn, " #%d 不符合Raft一致性规则而拒绝\n", rf.me)
		reply.Success = false
		reply.Term = rf.currentTerm

		if len(rf.log)+firstIndex-1 < args.PrevLogIndex {
			reply.ConflictTerm = -1
			reply.ConflictIndex = len(rf.log) + firstIndex
		} else {
			reply.ConflictTerm = rf.log[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			for index >= firstIndex && rf.log[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index + 1
		}

		return
	}

	//AE的更长或AE更短但是有不同
	//对于乱序到达的RPC，保证只会更新至最新状态、
	//也就是对于被rf.log包含的Entries不做处理
	flag := false
	len1 := len(rf.log) + firstIndex - 1 - args.PrevLogIndex
	len2 := len(args.Entries)
	if len1 < len2 {
		flag = true
	} else {
		j := args.PrevLogIndex - firstIndex + 1
		for i := 0; i < len2; i++ {
			if args.Entries[i].Term != rf.log[j].Term {
				flag = true
				break
			}
			j++
		}
	}

	if flag {
		rf.log = append(rf.log[0:args.PrevLogIndex+1-firstIndex], args.Entries...)
	}

	//更新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		tmp := rf.commitIndex
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1+firstIndex)

		if rf.commitIndex > tmp {
			rf.applyCond.Signal()
		}
	}
	reply.Success = true
	reply.Term = rf.currentTerm
}

// AppendEntries call
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//Call 收到信息返回true，time-out收不到返回false
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok == true {
		return true
	} else {
		return false
	}
}

func (rf *Raft) replicate(isHeartBeat bool) {
	LOG_RAFT(Info, " #%d replicate, term: %d\n", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if isHeartBeat {
			//简单心跳
			go rf.replicateToPeer(i)
		} else {
			//唤醒协程进行复制
			rf.replicatorCond[i].Signal()
		}
	}
}

func (rf *Raft) replicator(peer int) {
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for rf.killed() == false {
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateToPeer(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//只要没确认到最新就要接着发
	return rf.state == "leader" && rf.matchIndex[peer] < rf.log[len(rf.log)-1].Index
}

// 对某个单一节点发送属于他的AE
func (rf *Raft) replicateToPeer(peer int) {
	rf.mu.Lock()
	if rf.state != "leader" {
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[peer] <= rf.log[0].Index {
		//Snapshot
		var args *InstallSnapshotArgs
		args = rf.getInstallSnapshotArgs(peer)
		rf.mu.Unlock()

		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.ChangeState("follower")
				rf.votedFor = -1
				rf.electionTimer.Reset(getRandomTimeout())

				rf.persist()
				return
			}

			if rf.currentTerm == args.Term && rf.state == "leader" {
				//一定成功
				tmpMatchIndex := args.LastIncludedIndex
				tmpNextIndex := tmpMatchIndex + 1

				if rf.nextIndex[peer] < tmpNextIndex {
					rf.nextIndex[peer] = tmpNextIndex
					rf.matchIndex[peer] = tmpMatchIndex
				}
			}
			//判断是否可以更新commitIndex
			for i := len(rf.log) - 1; rf.log[i].Index > rf.commitIndex && rf.log[i].Term == rf.currentTerm; i-- {
				cnt := 0
				for j := range rf.peers {
					if rf.matchIndex[j] >= rf.log[i].Index {
						cnt++
					}
				}
				if cnt > len(rf.peers)/2 {
					rf.commitIndex = rf.log[i].Index
					LOG_RAFT(Debug, " leader commitIndex update %d\n", i)
					rf.applyCond.Signal()
					break
				}
			}
		}
	} else {
		//AE
		var args *AppendEntriesArgs
		args = rf.getAppendEntriesArgs(peer)
		rf.mu.Unlock()

		reply := new(AppendEntriesReply)
		if rf.sendAppendEntries(peer, args, reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.ChangeState("follower")
				rf.votedFor = -1
				rf.electionTimer.Reset(getRandomTimeout())

				rf.persist()
				return
			}

			if rf.currentTerm == args.Term && rf.state == "leader" && args.PrevLogIndex == rf.nextIndex[peer]-1 {
				if reply.Success == false {
					if reply.Term == 0 {
						return
					}

					//因为raft不一致导致的失败
					//这里要进行一些优化，采用在AE参数中增加ConflictTerm, ConflictIndex的方法
					if reply.ConflictTerm == -1 {
						rf.nextIndex[peer] = reply.ConflictIndex //没有在follower中出现
					} else {
						//不包含ConflictTerm，nextIndex[peer]置为ConflictIndex
						//包含ConflictTerm, nextIndex[peer]置为本地在ConflictTerm的entry的后面
						conflictTermIndex := -1
						for index := args.PrevLogIndex - rf.log[0].Index; index > 0; index-- {
							if rf.log[index-1].Term == reply.ConflictTerm {
								conflictTermIndex = index
								break
							}
							if rf.log[index-1].Term < reply.ConflictTerm {
								break
							}
						}
						if conflictTermIndex != -1 {
							rf.nextIndex[peer] = conflictTermIndex
						} else {
							rf.nextIndex[peer] = reply.ConflictIndex
						}
					}

					//应该立即重试，防止不一致的发生，而不是等到下个心跳或AE
					rf.replicatorCond[peer].Signal()
				} else {
					//只要返回成功，就说明已经成功该follower与leader同步
					//成功复制，更新nextIndex, matchIndex
					//注意只能把commitIndex设置在本term提交的范围内
					//在本任期第一次提交之前，commitIndex=lastApplied=0
					//本次提交之后才能改变commitIndex

					tmpMatchIndex := args.PrevLogIndex + len(args.Entries)
					tmpNextIndex := tmpMatchIndex + 1

					if rf.nextIndex[peer] < tmpNextIndex {
						rf.nextIndex[peer] = tmpNextIndex
						rf.matchIndex[peer] = tmpMatchIndex
					}
				}
				//判断是否可以更新commitIndex
				for i := len(rf.log) - 1; rf.log[i].Index > rf.commitIndex && rf.log[i].Term == rf.currentTerm; i-- {
					cnt := 0
					for j := range rf.peers {
						if rf.matchIndex[j] >= rf.log[i].Index {
							cnt++
						}
					}
					if cnt > len(rf.peers)/2 {
						rf.commitIndex = rf.log[i].Index
						LOG_RAFT(Debug, " leader commitIndex update %d\n", i)
						rf.applyCond.Signal()
						break
					}
				}
			}
		}
	}
}

// 获取AE参数
func (rf *Raft) getAppendEntriesArgs(peer int) *AppendEntriesArgs {
	args := new(AppendEntriesArgs)
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex

	idx := rf.nextIndex[peer] - rf.log[0].Index - 1
	args.PrevLogIndex = rf.log[idx].Index
	args.PrevLogTerm = rf.log[idx].Term

	//是否有待同步的日志
	if rf.nextIndex[peer] <= rf.log[len(rf.log)-1].Index {
		args.Entries = rf.log[rf.nextIndex[peer]-rf.log[0].Index:]
	}
	return args
}
