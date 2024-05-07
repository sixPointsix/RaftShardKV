package kvraft

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"gp/labgob"
	"gp/labrpc"
	"gp/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operation string
	Key       string
	Value     string
	ClientId  int64
	CommandId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int //防止回退

	KV          map[string]string         //key-value内存数据库
	clientsCh   map[int]chan *Result      //处理client的handler监听的channel
	lastCommand map[int64]LastCommandInfo //为每一个client维护它唯一提交的信息
}

// applier通过channel传递给client Goroutine的参数
// 其中包括是否成功，是否超时
type Result struct {
	Value string
	Err   Err
}

// 记录唯一提交的Id和那一刻的结果
type LastCommandInfo struct {
	CommandId int64
	Result    *Result
}

// 通过start函数传递给Raft层，监听channel直到收到回复
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	command := Op{"Get", args.Key, "", args.ClientId, args.CommandId}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getClientsCh(index)
	kv.mu.Unlock()

	//注意！阻塞的select不能持有锁
	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.Value = result.Value
	case <-time.After(time.Millisecond * 300):
		fmt.Printf("超时\n")
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.clientsCh, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	if kv.isDuplicated(args.ClientId, args.CommandId) {
		result := kv.lastCommand[args.ClientId].Result
		reply.Err = result.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	command := Op{
		Operation: args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		CommandId: args.CommandId,
	}

	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getClientsCh(index)
	kv.mu.Unlock()

	//注意！阻塞的select不能持有锁
	select {
	case result := <-ch:
		reply.Err = result.Err
	case <-time.After(time.Millisecond * 300):
		fmt.Printf("超时\n")
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		delete(kv.clientsCh, index)
		kv.mu.Unlock()
	}()
}

func (kv *KVServer) KVGet(key string) (string, Err) {
	if value, ok := kv.KV[key]; ok {
		return value, OK
	} else {
		return "", ErrNoKey
	}
}

func (kv *KVServer) KVPut(key string, value string) Err {
	kv.KV[key] = value
	return OK
}

func (kv *KVServer) KVAppend(key string, value string) Err {
	kv.KV[key] += value
	return OK
}

// 专用协程用于监听applyCh
// 注意要对状态机的操作要是幂等，通过CommandId实现
// 对每个client维护一个它的最后的CommadId
func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid == true {
				result := new(Result)
				command := applyMsg.Command.(Op)      //Op
				commandIndex := applyMsg.CommandIndex //index

				kv.mu.Lock()

				if commandIndex <= kv.lastApplied {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = commandIndex

				if command.Operation != "Get" && kv.isDuplicated(command.ClientId, command.CommandId) {
					//duplicated detection
					result = kv.lastCommand[command.ClientId].Result
				} else {
					fmt.Printf("#%d applier接收到日志条目, commandId:%d\n", kv.me, command.CommandId)
					//任何Get或者未apply过的Put/Append都应该执行
					if command.Operation == "Put" {
						result.Err = kv.KVPut(command.Key, command.Value)
					} else if command.Operation == "Append" {
						result.Err = kv.KVAppend(command.Key, command.Value)
					} else {
						result.Value, result.Err = kv.KVGet(command.Key)
					}

					if command.Operation != "Get" {
						kv.lastCommand[command.ClientId] = LastCommandInfo{command.CommandId, result}
					}
				}

				//只有leader才有clientsCh在等待
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && applyMsg.CommandTerm == currentTerm {
					ch := kv.getClientsCh(commandIndex)
					ch <- result
				}
				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					fmt.Printf("#%d 生成快照\n", kv.me)
					kv.takeSnapshot(commandIndex)
				}
				kv.mu.Unlock()
			} else if applyMsg.SnapshotValid {
				fmt.Printf("#%d applier收到快照\n", kv.me)
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
					kv.restoreSnapshot(applyMsg.Snapshot)
					kv.lastApplied = applyMsg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				//意外
				fmt.Printf("#%d Panic!\n", kv.me)
			}
		}
	}
}

func (kv *KVServer) isDuplicated(clientId int64, commandId int64) bool {
	lastCommandInfo, ok := kv.lastCommand[clientId]
	return ok && commandId <= lastCommandInfo.CommandId
}

func (kv *KVServer) getClientsCh(index int) chan *Result {
	if _, ok := kv.clientsCh[index]; !ok {
		kv.clientsCh[index] = make(chan *Result, 1)
	}
	return kv.clientsCh[index]
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.maxraftstate <= kv.rf.GetRaftStateSize()
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KV)
	e.Encode(kv.lastCommand)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var KV map[string]string
	var lastCommand map[int64]LastCommandInfo
	if d.Decode(&KV) != nil ||
		d.Decode(&lastCommand) != nil {
		fmt.Printf("Snapshot decode failed!\n")
	}
	kv.KV, kv.lastCommand = KV, lastCommand
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.KV = make(map[string]string)
	kv.clientsCh = make(map[int]chan *Result)
	kv.lastCommand = make(map[int64]LastCommandInfo)

	kv.restoreSnapshot(persister.ReadSnapshot())

	go kv.applier()

	return kv
}
