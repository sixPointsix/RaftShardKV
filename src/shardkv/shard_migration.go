package shardkv

import (
	"sync"
)

type PullDataArgs struct {
	ConfNum  int
	ShardIds []int
}

type PullDataReply struct {
	Err     Err
	ConfNum int
	Shards  map[int]*Shard
}

// get all "Pulling" State Shards
// pull from remote server
func (kv *ShardKV) migrationAction() {
	kv.mu.Lock()
	gid2shardIDs := kv.getShardIDsByStatus(Pulling, &kv.lastConfig)
	if len(gid2shardIDs) == 0 {
		kv.mu.Unlock()
		return
	}
	// concurently pull
	var wg sync.WaitGroup
	for gid, shardIDs := range gid2shardIDs {
		wg.Add(1)
		servers := kv.lastConfig.Groups[gid]
		go func(servers []string, configNum int, shardIDs []int) {
			defer wg.Done()
			args := PullDataArgs{
				ConfNum:  configNum,
				ShardIds: shardIDs,
			}
			for _, server := range servers {
				var resp PullDataReply
				srv := kv.makeEnd(server)
				if srv.Call("ShardKV.GetShardsData", &args, &resp) && resp.Err == OK {
					kv.Execute(NewInsertShardsCommand(&resp), &OpResp{})
				}
			}
		}(servers, kv.currentConfig.Num, shardIDs)
	}
	kv.mu.Unlock()
	LOG(Server, " #%d(gid: %d) migrationAction wait", kv.me, kv.gid)
	wg.Wait()
	LOG(Server, " #%d(gid: %d) migrationAction done", kv.me, kv.gid)
}

func (kv *ShardKV) GetShardsData(args *PullDataArgs, reply *PullDataReply) {
	defer LOG(Server, " #%d(gid: %d) GetShardsData: args: %+v reply: %+v", kv.me, kv.gid, args, reply)
	// only pull shards from leader
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()

	if kv.currentConfig.Num < args.ConfNum {
		reply.Err = ErrNotReady
		kv.mu.Unlock()
		kv.configureAction()
		return
	}

	reply.Shards = make(map[int]*Shard)
	for _, shardID := range args.ShardIds {
		reply.Shards[shardID] = kv.shards[shardID].deepCopy()
	}

	reply.ConfNum, reply.Err = args.ConfNum, OK
	kv.mu.Unlock()
}

// after raft, apply to kv statemachine
func (kv *ShardKV) applyInsertShards(shardsInfo *PullDataReply) *OpResp {
	LOG(Server, " #%d(gid: %d) before applyInsertShards: %+v", kv.me, kv.gid, kv.shards)
	if shardsInfo.ConfNum == kv.currentConfig.Num {
		for shardId, shardData := range shardsInfo.Shards {
			if kv.shards[shardId].Status == Pulling {
				kv.shards[shardId] = shardData.deepCopy()
				kv.shards[shardId].Status = GCing
			} else {
				LOG(Warn, " #%d(gid: %d) shard %d is not Pulling: %+v", kv.me, kv.gid, shardId, kv.shards[shardId])
				break
			}
		}
		LOG(Server, " #%d(gid: %d) after applyInsertShards: %+v", kv.me, kv.gid, kv.shards)
		return &OpResp{OK, ""}
	}
	return &OpResp{ErrOutDated, ""}
}
