# MIT6.824

## Lab1

> Rules

1. 最后文件需要输出nReduce个，文件名格式为`mr-out-X`
2. 输出到文件的格式在`mrsequential.go`中
3. 只用写`worker.go`/`coordinator.go`/`rpc.go`这三个文件
4. worker将中间文件输出到当前文件夹下，之后worker执行reduce任务的时候从中取
5. 需要实现`coordinator.go`中的`Done()`方法，当全部任务被执行完了之后返回true，然后`mrcoordinator`退出
6. 当所有的任务的完成的时候，worker也应该停止。简单的方法就是使用rpc的回调，当返回err，也就是故障的时候，这里可以理解为coordinator已经结束了，所以这时候worker可以退出了。

> Hints

1. 一开始可以先实现worker的worker方法来和coordinator进行rpc调用来获取任务，coordinator回应给他文件名作为一个还未开始的map任务。然后worker读取这些文件然后调用map方法，参考`mrsequential.go`中的方法
2. map和reduce方法是作为插件使用的，记得启动的时候带上参数`wc.so`
3. 没有`wc.so`插件就build一个，`go build -race -buildmode=plugin ../mrapps/wc.go`
4. 中间文件可以命名为`mr-X-Y`，X表示map任务编号，Y表示reduce任务编号。
5. 中间文件使用json方法来存来读取
6. map的worker使用`ihash(key)`方法来获取reduce编号，靠这个存到对应的中间文件中
7. coordinator作为一个rpc服务端，需要对共享资源进行并发保护
8. run的时候使用-race检查一下
9. 只有所有的map任务被执行完了之后才能进行reduce任务的分配。一种方法是worker周期的请求coordinator来获取任务，没获取到就sleep一会再来请求。另一种方法是每个rpc的handler可以循环等待一下，使用`time.Sleep`或者`sync.Cond`
10. coordinator不能可靠的区别那些故障worker，包括那些执行的太慢的节点。最好就是每次分配了任务之后可以等待10秒，如果10秒都没有完成，就可以认为该worker已故障了。需要重新将该任务分配给别的worker。
11. 测试故障恢复可以使用`mrapps/crash.go`插件，会随机在map和reduce中故障
12. 为了不让已经故障的节点的产生文件对作为真正的中间文件，可以使用论文中提到的临时文件的方法，worker写的使用可以使用临时文件，使用`ioutil.TempFile`来创建临时文件，然后完成之后使用`os.rename`来原子性的命名。
13. 

---

### Task

> 结构

​		*任务的id(若为map类型则id为0-files.size-1，若为reduce类型则为分区序号，也就是0-nReduce-1)*

- `Id int`

  *任务的类型*

- `TaskType string`

  *任务需要的文件名(map任务使用，源文件名)*

- `Filename string`

  *任务需要的中间文件名集合(reduce任务使用)*

- `Filenames []string`

  *状态：free/working/finnished*

- `Status status`

  *Reduce任务数(也就是最终输出文件的分区数)*

- `NReduce int`

> 方法

```
//是否已完成
func (s *Task) isFinished() bool
//是否正在处理
func (s *Task) isWorking() bool 
//是否空闲(未被处理)
func (s *Task) isFree() bool
//修改状态为working
func (s *Task) working()
//修改状态为finished
func (s *Task) finished()
//修改状态为free
func (s *Task) free()
//是否是map任务
func (t *Task)isMapTask() bool
//是否是map任务
func (t *Task)isReduceTask() bool
```

---

### coordinator

> 结构

​		*存储map任务id->Task*

- `mapTasks map[int]*Task`
  *存储reduce任务id->*Task*
- `reduceTasks map[int]*Task`
  *map任务通道*
- `mapChan chan *Task`
  *reduce任务通道*
- `reduceChan chan *Task`
  *当前是否已经处理完所有map任务*
- `mapFinished bool`
  *互斥锁*
- `mu sync.Mutex`
  *中间文件reduceId->Filenames,format : mr-X-Y (X表示map的id,Y表示reduce的id)*
- `interFiles map[int][]string`

> 功能

1. 传入需要处理的文件名集合，和reduce数目n，n也就是最后出来的n个中间文件。实例化coordinator，创建map和reduce任务然后开启serve，等待worker前来取任务。
2. 有worker来取任务，判断当前的map通道是否为空，不为空则返回，为空则让它从reduce通道中取；如果此时已经有reduce任务了，则直接返回任务，若没有则直接返回；
3. 若worker来取的是map任务，则开启一个go routine等10秒后判断刚刚的Task的任务是否已经被完成，若还是working则将状态设为free，并且重新加入chan中。若取的是reduce也同理。
4. 当worker完成任务时，worker rpc调用finished方法，此时coordinator判断该任务的状态是否是working，是的话则完成该任务，更改状态为finished，作相应处理；map任务则将传来的中间文件名用来加入到中间文件map中；reduce任务则不作额外处理。
5. Done方法会不断检查当前所有的任务是否已经完成，当所有任务的状态为finished时，返回true。

---

### worker

> 功能

1. rpc调用coordinator的GetTask方法，若获取到Task则执行相应的方法，map或者reduce。若返回的Task为空的时候sleep一秒后再循环获取。若rpc调用返回err则worker可以退出了；
2. 当完成一个任务的时候，将完成的任务的情况rpc调用FinishedTask方法来通知coordinator。调用err的时候直接退出。
3. 有map和reduce方法，分别进行处理。
---
---
## Lab2

### Summary

Raft 是一种用来管理章节 2 中描述的复制日志的算法。图 2 为了参考之用，总结这个算法的简略版本，图 3 列举了这个算法的一些关键特性。图中的这些元素会在剩下的章节逐一介绍。

Raft 通过选举一个杰出的领导人，然后给予他全部的管理复制日志的责任来实现一致性。领导人从客户端接收日志条目（log entries），把日志条目复制到其他服务器上，并告诉其他的服务器什么时候可以安全地将日志条目应用到他们的状态机中。拥有一个领导人大大简化了对复制日志的管理。例如，领导人可以决定新的日志条目需要放在日志中的什么位置而不需要和其他服务器商议，并且数据都从领导人流向其他服务器。一个领导人可能会发生故障，或者和其他服务器失去连接，在这种情况下一个新的领导人会被选举出来。

通过领导人的方式，Raft 将一致性问题分解成了三个相对独立的子问题，这些问题会在接下来的子章节中进行讨论：

- **领导选举**：当现存的领导人发生故障的时候, 一个新的领导人需要被选举出来（章节 5.2）
- **日志复制**：领导人必须从客户端接收日志条目（log entries）然后复制到集群中的其他节点，并强制要求其他节点的日志和自己保持一致。
- **安全性**：在 Raft 中安全性的关键是在图 3 中展示的状态机安全：如果有任何的服务器节点已经应用了一个确定的日志条目到它的状态机中，那么其他服务器节点不能在同一个日志索引位置应用一个不同的指令。章节 5.4 阐述了 Raft 算法是如何保证这个特性的；这个解决方案涉及到选举机制（5.2 节）上的一个额外限制。

在展示一致性算法之后，这一章节会讨论一些可用性的问题和计时在系统中的作用。

**状态**：

所有服务器上的持久性状态 (在响应 RPC 请求之前，已经更新到了稳定的存储设备)

| 参数        | 解释                                                         |
| ----------- | ------------------------------------------------------------ |
| currentTerm | 服务器已知最新的任期（在服务器首次启动时初始化为0，单调递增） |
| votedFor    | 当前任期内收到选票的 candidateId，如果没有投给任何候选人 则为空 |
| log[]       | 日志条目；每个条目包含了用于状态机的命令，以及领导人接收到该条目时的任期（初始索引为1） |

所有服务器上的易失性状态

| 参数        | 解释                                                         |
| ----------- | ------------------------------------------------------------ |
| commitIndex | 已知已提交的最高的日志条目的索引（初始值为0，单调递增）      |
| lastApplied | 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增） |

领导人（服务器）上的易失性状态 (选举后已经重新初始化)

| 参数         | 解释                                                         |
| ------------ | ------------------------------------------------------------ |
| nextIndex[]  | 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导人最后的日志条目的索引+1） |
| matchIndex[] | 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增） |

**追加条目（AppendEntries）RPC**：

由领导人调用，用于日志条目的复制，同时也被当做心跳使用

| 参数         | 解释                                                         |
| ------------ | ------------------------------------------------------------ |
| term         | 领导人的任期                                                 |
| leaderId     | 领导人 ID 因此跟随者可以对客户端进行重定向（译者注：跟随者根据领导人 ID 把客户端的请求重定向到领导人，比如有时客户端把请求发给了跟随者而不是领导人） |
| prevLogIndex | 紧邻新日志条目之前的那个日志条目的索引                       |
| prevLogTerm  | 紧邻新日志条目之前的那个日志条目的任期                       |
| entries[]    | 需要被保存的日志条目（被当做心跳使用时，则日志条目内容为空；为了提高效率可能一次性发送多个） |
| leaderCommit | 领导人的已知已提交的最高的日志条目的索引                     |

| 返回值  | 解释                                                         |
| ------- | ------------------------------------------------------------ |
| term    | 当前任期，对于领导人而言 它会更新自己的任期                  |
| success | 如果跟随者所含有的条目和 prevLogIndex 以及 prevLogTerm 匹配上了，则为 true |

接收者的实现：

1. 返回假 如果领导人的任期小于接收者的当前任期（译者注：这里的接收者是指跟随者或者候选人）（5.1 节）
2. 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在 prevLogIndex 上能和 prevLogTerm 匹配上 （译者注：在接收者日志中 如果能找到一个和 prevLogIndex 以及 prevLogTerm 一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）（5.3 节）
3. 如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
4. 追加日志中尚未存在的任何新条目
5. 如果领导人的已知已提交的最高日志条目的索引大于接收者的已知已提交最高日志条目的索引（`leaderCommit > commitIndex`），则把接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为 领导人的已知已经提交的最高的日志条目的索引 leaderCommit 或者是 上一个新条目的索引 取两者的最小值

**请求投票（RequestVote）RPC**：

由候选人负责调用用来征集选票（5.2 节）

| 参数         | 解释                         |
| ------------ | ---------------------------- |
| term         | 候选人的任期号               |
| candidateId  | 请求选票的候选人的 ID        |
| lastLogIndex | 候选人的最后日志条目的索引值 |
| lastLogTerm  | 候选人最后日志条目的任期号   |

| 返回值      | 解释                                       |
| ----------- | ------------------------------------------ |
| term        | 当前任期号，以便于候选人去更新自己的任期号 |
| voteGranted | 候选人赢得了此张选票时为真                 |

接收者实现：

1. 如果`term < currentTerm`返回 false （5.2 节）
2. 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）

**所有服务器需遵守的规则**：

所有服务器：

- 如果`commitIndex > lastApplied`，则 lastApplied 递增，并将`log[lastApplied]`应用到状态机中（5.3 节）
- 如果接收到的 RPC 请求或响应中，任期号`T > currentTerm`，则令 `currentTerm = T`，并切换为跟随者状态（5.1 节）

跟随者（5.2 节）：

- 响应来自候选人和领导人的请求
- 如果在超过选举超时时间的情况之前没有收到**当前领导人**（即该领导人的任期需与这个跟随者的当前任期相同）的心跳/附加日志，或者是给某个候选人投了票，就自己变成候选人

候选人（5.2 节）：

- 在转变成候选人后就立即开始选举过程
  - 自增当前的任期号（currentTerm）
  - 给自己投票
  - 重置选举超时计时器
  - 发送请求投票的 RPC 给其他所有服务器
- 如果接收到大多数服务器的选票，那么就变成领导人
- 如果接收到来自新的领导人的附加日志（AppendEntries）RPC，则转变成跟随者
- 如果选举过程超时，则再次发起一轮选举

领导人：

- 一旦成为领导人：发送空的附加日志（AppendEntries）RPC（心跳）给其他所有的服务器；在一定的空余时间之后不停的重复发送，以防止跟随者超时（5.2 节）

- 如果接收到来自客户端的请求：附加条目到本地日志中，在条目被应用到状态机后响应客户端（5.3 节）

- 如果对于一个跟随者，最后日志条目的索引值大于等于 nextIndex（

  ```
  lastLogIndex ≥ nextIndex
  ```

  ），则发送从 nextIndex 开始的所有日志条目：

  - 如果成功：更新相应跟随者的 nextIndex 和 matchIndex
  - 如果因为日志不一致而失败，则 nextIndex 递减并重试

- 假设存在 N 满足`N > commitIndex`，使得大多数的 `matchIndex[i] ≥ N`以及`log[N].term == currentTerm` 成立，则令 `commitIndex = N`（5.3 和 5.4 节）

![image-20220115222201231](https://ther1sing3un-personal-resource.oss-cn-beijing.aliyuncs.com/typora/images/image-20220115222201231.png)

> 图 2：一个关于 Raft 一致性算法的浓缩总结（不包括成员变换和日志压缩）。

| 特性             | 解释                                                         |
| ---------------- | ------------------------------------------------------------ |
| 选举安全特性     | 对于一个给定的任期号，最多只会有一个领导人被选举出来（5.2 节） |
| 领导人只附加原则 | 领导人绝对不会删除或者覆盖自己的日志，只会增加（5.3 节）     |
| 日志匹配原则     | 如果两个日志在某一相同索引位置日志条目的任期号相同，那么我们就认为这两个日志从头到该索引位置之间的内容完全一致（5.3 节） |
| 领导人完全特性   | 如果某个日志条目在某个任期号中已经被提交，那么这个条目必然出现在更大任期号的所有领导人中（5.4 节） |
| 状态机安全特性   | 如果某一服务器已将给定索引位置的日志条目应用至其状态机中，则其他任何服务器在该索引位置不会应用不同的日志条目（5.4.3 节） |

![image-20220115222136707](https://ther1sing3un-personal-resource.oss-cn-beijing.aliyuncs.com/typora/images/image-20220115222136707.png)

> 图 3：Raft 在任何时候都保证以上的各个特性。

### 5.1 Raft 基础

一个 Raft 集群包含若干个服务器节点；5 个服务器节点是一个典型的例子，这允许整个系统容忍 2 个节点失效。在任何时刻，每一个服务器节点都处于这三个状态之一：领导人、跟随者或者候选人。在通常情况下，系统中只有一个领导人并且其他的节点全部都是跟随者。跟随者都是被动的：他们不会发送任何请求，只是简单的响应来自领导人或者候选人的请求。领导人处理所有的客户端请求（如果一个客户端和跟随者联系，那么跟随者会把请求重定向给领导人）。第三种状态，候选人，是用来在 5.2 节描述的选举新领导人时使用。图 4 展示了这些状态和他们之间的转换关系；这些转换关系会在接下来进行讨论。

![image-20220115222119698](https://ther1sing3un-personal-resource.oss-cn-beijing.aliyuncs.com/typora/images/image-20220115222119698.png)

