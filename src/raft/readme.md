#### Lab2A
```
血的教训，写之前懒得看锁和架构的建议了，导致把rpc请求锁进去了，导致定时器无法定时计时，debug到吐血
```

#### Lab2B
```
由于先看了Guide，所以比较顺利。
导致bug的是对AppendEntries和HeartBeat的取舍，其中AppendEntries返回的Success一定代表了follower和leader的完全同步.
只要有last log index > nextIndex[i]，发送的AE就一定要携带log.
`否则可能会出现HB的Leadercommit使符合Raft的一致性规则但是还没有进行匹配的follower提交了错误的日志
```

#### Lab2c
```
测试用例6，Figure8 unreliable有1/10左右的概率过不去，其余没什么问题。
暂时怀疑是性能问题

参考 https://github.com/OneSizeFitsQuorum/MIT6.824-2021/blob/master/docs/lab2.md
对复制模型进行了修改。将日志复制触发用协程的监听触发，而不是每次命令都进行一轮复制
减少了网络之中的RPC数量。
```
