package shardkv

import "github.com/sixPointsix/RaftShardKV/shardctrler"

// detect confiration change
func (kv *ShardKV) configureAction() {
	canPerformNextConfig := true
	kv.mu.Lock()
	// need all shards is "Serving" state
	// or wait for shard_migration over
	for _, shard := range kv.shards {
		if shard.Status != Serving {
			LOG(Warn, "G%+v S%d shard: %+v", kv.me, kv.gid, shard)
			canPerformNextConfig = false
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.Unlock()
	if canPerformNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			kv.Execute(NewConfigurationCommand(&nextConfig), &OpResp{})
		}
	} else {
		LOG(Warn, " #%d(gid: %d) don't need fetch config!", kv.me, kv.gid)
	}
}

// apply new configuration to StateMachine
func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *OpResp {
	if nextConfig.Num == kv.currentConfig.Num+1 {
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig.DeepCopy()
		kv.currentConfig = nextConfig.DeepCopy()
		LOG(Warn, " #%d(gid: %d) applyConfiguration %d is %+v", kv.me, kv.gid, nextConfig.Num, nextConfig)
		return &OpResp{OK, ""}
	}
	return &OpResp{ErrTimeoutReq, ""}
}

func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	// special judge for first configuration
	if nextConfig.Num == 1 {
		shards := kv.getAllShards(nextConfig)
		for _, shard := range shards {
			kv.shards[shard] = NewShard(Serving)
		}
		return
	}

	newShards := kv.getAllShards(nextConfig)
	nowShards := kv.getAllShards(&kv.currentConfig)
	// lose shard
	for _, nowShard := range nowShards {
		if nextConfig.Shards[nowShard] != kv.gid {
			// BePulling
			kv.shards[nowShard].Status = BePulling
		}
	}
	// get shard
	for _, newShard := range newShards {
		if kv.currentConfig.Shards[newShard] != kv.gid {
			// Pulling -> GCing -> Serving
			kv.shards[newShard] = NewShard(Pulling)
		}
	}
}

func (kv *ShardKV) getAllShards(nextConfig *shardctrler.Config) []int {
	var shards []int
	for shard, gid := range nextConfig.Shards {
		if gid == kv.gid {
			shards = append(shards, shard)
		}
	}
	return shards
}

func (kv *ShardKV) getShardIDsByStatus(status ShardStatus, config *shardctrler.Config) map[int][]int {
	gid2shardIDs := make(map[int][]int)
	for shard, _ := range kv.shards {
		if kv.shards[shard].Status == status {
			gid := config.Shards[shard]
			if _, ok := gid2shardIDs[gid]; !ok {
				vec := [1]int{shard}
				gid2shardIDs[gid] = vec[:]
			} else {
				gid2shardIDs[gid] = append(gid2shardIDs[gid], shard)
			}
		}
	}
	return gid2shardIDs
}
