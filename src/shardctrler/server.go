package shardctrler

import (
	"sort"
	"sync"
	"time"

	"gp/labgob"
	"gp/labrpc"
	"gp/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	lastApplied int

	configs     []Config // indexed by config num
	clientsCh   map[int]chan *Result
	lastCommand map[int64]LastCommandInfo
}

type Op struct {
	// Your data here.
	Operation string
	ClientId  int64
	CommandId int64

	Servers map[int][]string //join
	GIDs    []int            //leave
	Shard   int              //move
	GID     int              //move
	Num     int              //query
}

type Result struct {
	Err    Err
	Config Config
}

type LastCommandInfo struct {
	CommandId int64
	Result    *Result
}

/*
type Config struct {
	Num int
	Shards [NShards]int
	Groups map[int][]string
}
*/

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	command := Op{"Join", args.ClientId, args.CommandId, args.Servers, nil, 0, 0, 0}

	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := sc.getClientsCh(index)
	sc.mu.Unlock()

	//注意！阻塞的select不能持有锁
	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.WrongLeader = false
	case <-time.After(time.Millisecond * 300):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.clientsCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	command := Op{"Leave", args.ClientId, args.CommandId, nil, args.GIDs, 0, 0, 0}

	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := sc.getClientsCh(index)
	sc.mu.Unlock()

	//注意！阻塞的select不能持有锁
	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.WrongLeader = false
	case <-time.After(time.Millisecond * 300):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.clientsCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	command := Op{"Move", args.ClientId, args.CommandId, nil, nil, args.Shard, args.GID, 0}

	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := sc.getClientsCh(index)
	sc.mu.Unlock()

	//注意！阻塞的select不能持有锁
	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.WrongLeader = false
	case <-time.After(time.Millisecond * 300):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.clientsCh, index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	command := Op{"Query", args.ClientId, args.CommandId, nil, nil, 0, 0, args.Num}

	index, _, isLeader := sc.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := sc.getClientsCh(index)
	sc.mu.Unlock()

	//注意！阻塞的select不能持有锁
	select {
	case result := <-ch:
		reply.Err = result.Err
		reply.Config = result.Config
		reply.WrongLeader = false
	case <-time.After(time.Millisecond * 300):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		delete(sc.clientsCh, index)
		sc.mu.Unlock()
	}()
}

// join和leave需要考虑分片的负载均衡
func (sc *ShardCtrler) ConfigJoin(groups map[int][]string) Err {
	lastConfig := sc.configs[len(sc.configs)-1]
	config := Config{len(sc.configs), lastConfig.Shards, deepCopy(lastConfig.Groups)}

	for gid, servers := range groups {
		if _, ok := config.Groups[gid]; !ok {
			//如果原配置中没有该gid，就加进来
			//深拷贝的[]string
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			config.Groups[gid] = newServers
		}
	}

	//rebalance
	hash := make(map[int][]int) //gid->shard的映射
	for gid := range config.Groups {
		hash[gid] = make([]int, 0)
	}
	for shardId, gid := range config.Shards {
		hash[gid] = append(hash[gid], shardId)
	}

	for {
		max_gid, min_gid := getMax(hash), getMin(hash)
		if max_gid != 0 && len(hash[max_gid])-len(hash[min_gid]) <= 1 {
			break
		}

		//只有gid=0或多的至少比少的大2时才会转移
		//每次转移一个
		hash[min_gid] = append(hash[min_gid], hash[max_gid][0])
		hash[max_gid] = hash[max_gid][1:]
	}

	var newShards [NShards]int
	for gid, shards := range hash {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	config.Shards = newShards
	sc.configs = append(sc.configs, config)
	LOG_SC(Info, " new config: %v\n", sc.configs[len(sc.configs)-1])

	return OK
}

func (sc *ShardCtrler) ConfigLeave(gids []int) Err {
	lastConfig := sc.configs[len(sc.configs)-1]

	tmpGroups := deepCopy(lastConfig.Groups)
	config := Config{len(sc.configs), lastConfig.Shards, tmpGroups}

	hash := make(map[int][]int) //gid->shard的映射
	for gid := range config.Groups {
		hash[gid] = make([]int, 0)
	}
	for shardId, gid := range config.Shards {
		hash[gid] = append(hash[gid], shardId)
	}

	tmpShards := make([]int, 0) //leave后暂未分配的shards
	for _, gid := range gids {
		if _, ok := config.Groups[gid]; ok {
			delete(config.Groups, gid)
		}
		if shards, ok := hash[gid]; ok {
			tmpShards = append(tmpShards, shards...)
			delete(hash, gid)
		}
	}

	//reassign
	var newShards [NShards]int
	if len(config.Groups) > 0 {
		for _, shard := range tmpShards {
			min_gid := getMin(hash)
			hash[min_gid] = append(hash[min_gid], shard)
		}

		for gid, shards := range hash {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}

	config.Shards = newShards
	sc.configs = append(sc.configs, config)
	LOG_SC(Info, "new config: %v\n", sc.configs[len(sc.configs)-1])

	return OK
}

// move和query直接实现即可
func (sc *ShardCtrler) ConfigMove(shard int, gid int) Err {
	lastConfig := sc.configs[len(sc.configs)-1]

	//深拷贝，防止slice的引用
	tmpGroups := deepCopy(lastConfig.Groups)
	config := Config{len(sc.configs), lastConfig.Shards, tmpGroups}
	config.Shards[shard] = gid
	sc.configs = append(sc.configs, config)
	LOG_SC(Info, "new config: %v\n", sc.configs[len(sc.configs)-1])
	return OK
}

func (sc *ShardCtrler) ConfigQuery(num int) (Config, Err) {
	lastIndex := len(sc.configs) - 1
	if num == -1 || num > lastIndex {
		return sc.configs[lastIndex], OK
	} else {
		return sc.configs[num], OK
	}
}

func deepCopy(groups map[int][]string) map[int][]string {
	tmpGroups := make(map[int][]string)
	for key, value := range groups {
		tmpServers := make([]string, len(value))
		copy(tmpServers, value)
		tmpGroups[key] = tmpServers
	}
	return tmpGroups
}

// 得到最多shard的gid
func getMax(hash map[int][]int) int {
	//gid == 0 是无效配置，因为一开始所有的分片都会分给gid=0的
	//所以要优先把gid=0的分走
	if shards, ok := hash[0]; ok && len(shards) > 0 {
		return 0
	}

	//由于map遍历的不确定性，可能再多个状态机上有不同
	//所以要确定遍历顺序
	var gids []int
	for gid := range hash {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	index := -1
	maxv := -1
	for _, gid := range gids {
		if len(hash[gid]) > maxv {
			index = gid
			maxv = len(hash[gid])
		}
	}

	return index
}

// 得到最少shard的gid
func getMin(hash map[int][]int) int {
	var gids []int
	for gid := range hash {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	index := -1
	minv := 100
	for _, gid := range gids {
		if gid != 0 && len(hash[gid]) < minv {
			index = gid
			minv = len(hash[gid])
		}
	}

	return index
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid == true {
				result := new(Result)
				command := applyMsg.Command.(Op)      //Op
				commandIndex := applyMsg.CommandIndex //index

				sc.mu.Lock()
				if commandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = commandIndex

				if command.Operation != "Query" && sc.isDuplicated(command.ClientId, command.CommandId) {
					//duplicated detection
					result = sc.lastCommand[command.ClientId].Result
				} else {
					LOG_SC(Debug, " #%d applier接收到日志条目, command:%v\n", sc.me, command)
					//任何Get或者未apply过的Put/Append都应该执行
					if command.Operation == "Join" {
						result.Err = sc.ConfigJoin(command.Servers)
					} else if command.Operation == "Leave" {
						result.Err = sc.ConfigLeave(command.GIDs)
					} else if command.Operation == "Move" {
						result.Err = sc.ConfigMove(command.Shard, command.GID)
					} else {
						result.Config, result.Err = sc.ConfigQuery(command.Num)
					}

					if command.Operation != "Query" {
						sc.lastCommand[command.ClientId] = LastCommandInfo{command.CommandId, result}
					}
				}

				//只有leader才有clientsCh在等待
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && applyMsg.CommandTerm == currentTerm {
					ch := sc.getClientsCh(commandIndex)
					ch <- result
				}
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) isDuplicated(clientId int64, commandId int64) bool {
	lastCommandInfo, ok := sc.lastCommand[clientId]
	return ok && commandId <= lastCommandInfo.CommandId
}

func (sc *ShardCtrler) getClientsCh(index int) chan *Result {
	if _, ok := sc.clientsCh[index]; !ok {
		sc.clientsCh[index] = make(chan *Result, 1)
	}
	return sc.clientsCh[index]
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientsCh = make(map[int]chan *Result)
	sc.lastCommand = make(map[int64]LastCommandInfo)

	//一开始只有configs[0]
	sc.configs = make([]Config, 1)
	sc.configs[0].Num = 0
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}
	sc.configs[0].Groups = make(map[int][]string)

	go sc.applier()

	return sc
}
