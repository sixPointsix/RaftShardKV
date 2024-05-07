package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sixPointsix/RaftShardKV/labgob"
	"github.com/sixPointsix/RaftShardKV/labrpc"
	"github.com/sixPointsix/RaftShardKV/raft"
	"github.com/sixPointsix/RaftShardKV/shardctrler"
)

const threshold float32 = 0.8
const snapshotLogGap int = 10

const (
	ConfigureMonitorTimeout        time.Duration = time.Duration(50) * time.Millisecond
	MigrationMonitorTimeout        time.Duration = time.Duration(50) * time.Millisecond
	GCMonitorTimeout               time.Duration = time.Duration(50) * time.Millisecond
	checkEntryInCurrentTermTimeout time.Duration = time.Duration(100) * time.Millisecond
)

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	dead         int32 // set by Kill()
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big

	shards       map[int]*Shard
	clientsCh    map[IndexAndTerm]chan OpResp
	lastApplied  int
	lastSnapshot int

	// for shardctrler
	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config
	sc            *shardctrler.Clerk
}

// client command handler
func (kv *ShardKV) Command(args *CmdArgs, reply *CmdReply) {
	defer LOG(Info, " #%d(gid: %d) args: %+v reply: %+v", kv.me, kv.gid, args, reply)

	kv.mu.Lock()
	shardID := key2shard(args.Key)
	if !kv.canServe(shardID) {
		LOG(Warn, " #%d(gid: %d) shard %d is %+v, can't servering(%+v)", kv.me, kv.gid, shardID, kv.shards[shardID], kv.currentConfig.Shards[shardID])
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if args.OpType != OpGet && kv.isDuplicate(shardID, args.ClientId, args.SeqId) {
		context := kv.shards[shardID].LastCmdContext[args.ClientId]
		reply.Value, reply.Err = context.Reply.Value, context.Reply.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	var resp OpResp
	kv.Execute(NewOperationCommand(args), &resp)
	reply.Value, reply.Err = resp.Value, resp.Err
}

// only Serving & BePulling state shards can serve
func (kv *ShardKV) canServe(shardID int) bool {
	return kv.currentConfig.Shards[shardID] == kv.gid &&
		(kv.shards[shardID].Status == Serving || kv.shards[shardID].Status == GCing)
}

// a channel for each client command, stored in clientsCh[]
func (kv *ShardKV) Execute(cmd Command, reply *OpResp) {
	index, term, is_leader := kv.rf.Start(cmd)
	if !is_leader {
		reply.Value, reply.Err = "", ErrWrongLeader
		return
	}

	kv.mu.Lock()
	it := IndexAndTerm{index, term}
	ch := make(chan OpResp, 1)
	kv.clientsCh[it] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.clientsCh, it)
		kv.mu.Unlock()
		close(ch)
	}()

	t := time.NewTimer(cmd_timeout)
	defer t.Stop()

	for {
		kv.mu.Lock()
		select {
		case resp := <-ch:
			reply.Value, reply.Err = resp.Value, resp.Err
			kv.mu.Unlock()
			return
		case <-t.C:
		priority:
			for {
				select {
				case resp := <-ch:
					reply.Value, reply.Err = resp.Value, resp.Err
					kv.mu.Unlock()
					return
				default:
					break priority
				}
			}
			reply.Value, reply.Err = "", ErrTimeout
			kv.mu.Unlock()
			return
		default:
			kv.mu.Unlock()
			time.Sleep(gap_time)
		}
	}
}

// after raft layer
// ShardKV apply to StateMachine
func (kv *ShardKV) applier() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.SnapshotValid { // if snapshot
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					kv.setSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex

				var resp OpResp
				command := msg.Command.(Command)
				switch command.Op {
				case Operation: // get/put/append
					cmd := command.Data.(CmdArgs)
					resp = *kv.applyOperation(&msg, &cmd)
				case Configuration: // change configuration
					nextConfig := command.Data.(shardctrler.Config)
					resp = *kv.applyConfiguration(&nextConfig)
				case InsertShards: // shard migration
					insertResp := command.Data.(PullDataReply)
					resp = *kv.applyInsertShards(&insertResp)
				case DeleteShards: // GC
					deleteResp := command.Data.(PullDataArgs)
					resp = *kv.applyDeleteShards(&deleteResp)
				}

				term, isLeader := kv.rf.GetState()

				if !isLeader || term != msg.CommandTerm {
					kv.mu.Unlock()
					continue
				}

				// after apply, reply clientsCh
				it := IndexAndTerm{msg.CommandIndex, term}
				ch, ok := kv.clientsCh[it]
				if ok {
					select {
					case ch <- resp:
					case <-time.After(10 * time.Millisecond):
					}
				}

				kv.mu.Unlock()
			} else {
			}
		default:
			time.Sleep(gap_time)
		}
	}
}

func (kv *ShardKV) applyOperation(msg *raft.ApplyMsg, cmd *CmdArgs) *OpResp {
	shardID := key2shard(cmd.Key)
	if kv.canServe(shardID) {
		if cmd.OpType != OpGet && kv.isDuplicate(shardID, cmd.ClientId, cmd.SeqId) {
			context := kv.shards[shardID].LastCmdContext[cmd.ClientId]
			return &context.Reply
		} else {
			var resp OpResp
			resp.Value, resp.Err = kv.Opt(cmd, shardID)
			kv.shards[shardID].LastCmdContext[cmd.ClientId] = OpContext{
				SeqId: cmd.SeqId,
				Reply: resp,
			}
			return &resp
		}
	}
	return &OpResp{ErrWrongGroup, ""}
}

func (kv *ShardKV) isDuplicate(shardId int, clientId int64, seqId int64) bool {
	context, ok := kv.shards[shardId].LastCmdContext[clientId]
	if !ok {
		return false
	}
	if seqId <= context.SeqId {
		return true
	}
	return false
}

// event monitor
/*
 * (1) 配置更新：从shardctler处轮询得到
 * (2) 分片迁移：检测Pulling状态的shards
 * (3) 垃圾回收：检测GCing状态的shards
 */
func (kv *ShardKV) startMonitor() {
	go kv.monitor(kv.configureAction, ConfigureMonitorTimeout)
	go kv.monitor(kv.migrationAction, MigrationMonitorTimeout)
	go kv.monitor(kv.gcAction, GCMonitorTimeout)
}

func (kv *ShardKV) monitor(action func(), timeout time.Duration) {
	for kv.killed() == false {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

// for snapshot
func (kv *ShardKV) snapshoter() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.isNeedSnapshot() {
			kv.doSnapshot(kv.lastApplied)
			kv.lastSnapshot = kv.lastApplied
		}
		kv.mu.Unlock()
		time.Sleep(snapshot_gap_time)
	}
}

func (kv *ShardKV) isNeedSnapshot() bool {
	for _, shard := range kv.shards {
		if shard.Status == BePulling {
			return false
		}
	}

	if kv.maxraftstate != -1 {
		if kv.rf.RaftPersistSize() > int(threshold*float32(kv.maxraftstate)) ||
			kv.lastApplied > kv.lastSnapshot+snapshotLogGap {
			return true
		}
	}
	return false
}

func (kv *ShardKV) doSnapshot(commandIndex int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(kv.shards) != nil ||
		e.Encode(kv.lastConfig) != nil ||
		e.Encode(kv.currentConfig) != nil {
		panic("server doSnapshot encode error")
	}
	kv.rf.Snapshot(commandIndex, w.Bytes())
}

func (kv *ShardKV) setSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shards map[int]*Shard
	var lastconfig, currentConfig shardctrler.Config

	if d.Decode(&shards) != nil ||
		d.Decode(&lastconfig) != nil ||
		d.Decode(&currentConfig) != nil {
		log.Fatalf("server setSnapshot decode error\n")
	} else {
		var str string
		for shardID, shard := range shards {
			desc := fmt.Sprintf("[%d : %+v]\n ", shardID, shard)
			str += desc
		}
		LOG(Warn, " #%d(gid: %d) snapshot read: %+v", kv.me, kv.gid, str)
		kv.shards = shards
		kv.lastConfig = lastconfig
		kv.currentConfig = currentConfig
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.doSnapshot(kv.lastApplied)
	kv.rf.Kill()
	LOG(Warn, " #%d(gid: %d) close shards: %+v config: %+v", kv.me, kv.gid, kv.shards, kv.currentConfig)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *ShardKV {
	// RPC ragister
	labgob.Register(Command{})
	labgob.Register(CmdArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(PullDataReply{})
	labgob.Register(PullDataArgs{})

	// kv init
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = makeEnd
	kv.gid = gid

	kv.applyCh = make(chan raft.ApplyMsg, 5)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.sc = shardctrler.MakeClerk(ctrlers)

	kv.shards = make(map[int]*Shard)
	kv.clientsCh = make(map[IndexAndTerm]chan OpResp)
	kv.lastApplied = 0
	kv.lastSnapshot = 0

	// load data from persister first
	kv.setSnapshot(persister.ReadSnapshot())

	// long-time goroutines
	go kv.applier()
	go kv.snapshoter()
	kv.startMonitor()

	return kv
}
