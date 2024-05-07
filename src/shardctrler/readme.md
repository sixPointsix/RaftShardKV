#### Lab4A

```
基本思路与Lab3A相同，只不过server对KV的处理改为server对configs的处理
注意几点：
（1）不同RPC的reply参数不同，query多一个Config参数--->（bug来源）
（2）go中map是引用，所以要deepCopy
（3）go中map的迭代器迭代顺序不一定，所以要固定一个迭代顺序
（4）如何rebalance，这里参考他人做法:
        join: 循环中不断选择shard最多和最少的gid，最多的给最少的一个，直至相差<=1
        leave: 会导致一些shards重新分配，每次选择最少shard的gid组，给他一个
```
