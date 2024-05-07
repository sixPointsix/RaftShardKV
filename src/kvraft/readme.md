#### Lab3A
```
先是锁错位置了，而中途return时候忘记释放锁，导致死锁
然后是applier协程忘记在start中打开了
还有就是只有leader拥有阻塞中的clients handler协程
最后应该是map的make用不清楚，debug了两个小时，最后改写为函数就好了
isDuplicated, getClientsCh
```

#### Lab3B 
```
KVServer重启时也应该立即读取快照，我一开始是以为先Raft层感应再传给server层
但是这样就太晚了，导致出现线性不一致的问题。
后来重新看了一遍lab导读，我真是愚蠢到家了
```
