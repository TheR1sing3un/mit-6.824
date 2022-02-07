# MIT6.824

## Lab1

### Rules

1. 最后文件需要输出nReduce个，文件名格式为`mr-out-X`
2. 输出到文件的格式在`mrsequential.go`中
3. 只用写`worker.go`/`coordinator.go`/`rpc.go`这三个文件
4. worker将中间文件输出到当前文件夹下，之后worker执行reduce任务的时候从中取
5. 需要实现`coordinator.go`中的`Done()`方法，当全部任务被执行完了之后返回true，然后`mrcoordinator`退出
6. 当所有的任务的完成的时候，worker也应该停止。简单的方法就是使用rpc的回调，当返回err，也就是故障的时候，这里可以理解为coordinator已经结束了，所以这时候worker可以退出了。

### Hints

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

​		*任务的id(若为map类型则id为0~files.size-1，若为reduce类型则为分区序号，也就是0~nReduce-1)*

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
- 如果在超过选举超时时间的情况之前没有收到**当前领导人**（即该领导人的任期需与这个跟随者的当前任期相同）的心跳/附加日志，或者是没有给某个候选人投了票，就自己变成候选人

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

一个 Raft 集群包含若干个服务器节点；5 个服务器节点是一个典型的例子，这允许整个系统容忍 2 个节点失效。在任何时刻，每一个服务器节点都处于这三个状态之一：领导人、跟随者或者候选人。在通常情况下，系统中只有一个领导人并且其他的节点全部都是跟随者。跟随者都是被动的：他们不会发送任何请求，只是简单的响应来自领导人或者候选人的请求。领导人处理所有的客户端请求（如果一个客户端和跟随者联系，那么跟随者会把请求重定向给领导人）。第三种状态，候选人，是用来在 5.2 节描述的选举新领导人时使用。图 4 展示了这些状态和他们之间的转换关系；这些转换关系会在接下来进行讨论。

![image-20220115222119698](https://ther1sing3un-personal-resource.oss-cn-beijing.aliyuncs.com/typora/images/image-20220115222119698.png)

---

### Lab2a

#### Hints

1. 不可以直接跑你的Raft实现的代码；使用`go test -run 2A -race`。
2. 根据论文的图二。这个部分只需要你来关注发送和接受RequestVote RPCs、那些关系到选举的服务器规则，以及那些关系到选举的状态。
3. 为`Raft`增加领袖选举的相关状态在`raft.go`中，同时你也需要定义一个结构关于每个log entry。
4. 完善`RequestVoteArgs`和`RequestVoteReply`结构。修改`Make()`来创建一个后台协程来开始周期性的发送`RequestVote`来进行领袖选举当没有在超时时间内接受到别的节点的心跳。通过这个方式，一个节点会知道谁是领袖(如果已经有领袖的情况)，或者开启选举让自己成为领袖。实现`RequestVote()`来进行投票。
5. 实现心跳机制，定义一个`AppendEntries`的RPC struct，然后让领袖周期性的发送该请求。写一个`AppendEntries`的RPC方法来重置选举时间，这样服务器就不会在已经当选领袖情况下再次成为被选举。
6. 确保所有的选举不会总是同时开始。
7. 测试要求领袖发送心跳检测不应该超过每秒十次。
8. 测试要求你的Raft在五秒内选举出一个新的领袖当旧的领袖已经失败的时候。选举可能会进行多轮，为了防止分裂投票(当包丢失或者候选人不幸选择了相同的随机退出时间)必须选择足够短的的超时。
9. 论文的第5.2节提到选举超时的范围是150到300毫秒。这个范围只有在领导者发送心跳频率大大超过每150毫秒一次的情况下才有意义。因为测试者限制你每秒10次心跳，你必须使用比报纸上150到300毫秒更大的选举超时，但不能太大，因为那样你可能无法在5秒内选出领导者。
10. 您需要编写定期执行操作或在时间延迟之后执行操作的代码。最简单的方法是创建一个 goroutine，循环调用`time.Sleep()`。
11. 记得完成`Getstate()`
12. 测试会调用你的`rf.Kill()`当它永久的关闭一个实例时，你可以使用`rf.Kill()`来检查`Killed()`是否已经被调用。最好每个循环都来判断一下。

---

### Lab2b

#### Hints

1. 实现ledaer和follower的代码去增加新的日志条目，通过`go test -run 2B -race`来检验正确。
2. 首要目标是通过`TestBasicAgree2B`。从实现`Start()`方法开始，然后编写代码去发送和接收新的日志条目通过`AppendEntries`，以及参考图二(也就是那个经典的图)。
3. 需要实现选举限制(在论文的5.4.1)
4. 在早期的2b 测试中，不能达成一致意见的一个原因是，领导人还活着的情况下，依然重复进行选举。在选举计时器管理中寻找漏洞，或者没有在赢得选举后立即发送心跳。
5. 您的代码可能具有重复检查某些事件的循环。不要让这些循环不停顿地连续执行，因为这会使实现变慢，以至于测试失败。使用 Go 的条件变量，或者使用在每个循环的时候进行10ms睡眠。

---

### Lab2c

一个真正的实现会在每次raft改变的时候讲raft的持久化状态写入到磁盘中，以及当重新启动后在重启期间从磁盘中读取状态。你的实现不会真正的使用磁盘；而是，你会保存和恢复持久化状态从一个`Persister`对象(在`persister.go`中)。无论谁调用`Raft.Make()`都会提供一个拥有最近的初始化状态的`Persister`。Raft应该从这个`Persister`来初始化这些状态，以及应该使用这个`Persister`来每次保存raft的持久化状态每当状态改变的时候。使用`Persister`的`ReadRaftState()`和`SaveRaftState()`方法。

#### Task1

完成函数`persist()`和`readPersist()`通过增加代码来保存和恢复持久化状态。你需要编码(或者说“初始化”)这些状态以byte数组的形式为了能将其传递给`Persister`。使用`labgob`编码器；看一些那些`persist()`和`readPersist()`的注释。`labgob`就像Go语言的`gob`编码器，但是会打印错误信息当你尝试编码一些使用小写字段名的结构体。

#### Task2

在你的实现里那些改变了持久化状态的地方插入对`persist()`的调用。一旦你完成了，你应该会通过剩下的这些test。

#### Hints

1. 2C的许多test涉及到server失效和网络丢失rpc的请求和回复，这些事件是不确定的，可能你会很幸运的通过这些test，即使的代码还有bug。通常需要你多次运行test来暴露出这些bug。
2. 你可能需要优化你的nextIndex的备份通过一次备份多个条目。看一下论文的第七页末尾和第八页开头的灰色方框内的字。但是论文对细节含糊其辞，你需要借助6.824的Raft课程来完善。
3. 2C只要求你实现持久化(persistense)和日志快速恢复(fast log backtracking)，2C的test可能会因为之前的部分导致失败。即使你通过了2A和2B的代码，但是你可能还是会有选举和日志的bug在2C的test中被暴露。

---

### Lab2d

为了支持快照，我们需要在service层和Raft库之间的接口。Raft论文没有指定这个接口，因此很多设计都是可以的。为了一个简单的实现，我们决定使用接下来的接口：

- `Snapshot(index int, snapshot []byte)`
- `CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool`

service层调用`Snapshot()`将其状态传递给Raft。快照包含所有的信息以及index。这意味着相应的Raft节点不再需要日志。你的Raft实现应该尽可能的调整log。你必须修改你的Raft代码来只操作末尾的log。

正如Raft论文中讨论的那样，Raft的leaders必须通过安装快照的方式来告诉落后的Raft节点来更新它的的状态。你需要实现`InstallSnapshot`的RPC发送和处理为了安装快照。这和`AppendEntries`形成对比，`AppendEntries`发送日志条目然后一个个被service应用。

`InstallSnapshot`的RPC是在Raft节点间调用，而提供的框架函数`Snapshot/CondInstallSnapshot`是service用来和Raft通信的。

当follower接受并处理`InstallSnapshot`的RPC请求时，它必须使用Raft将包含的快照交给service。`InstallSnapshot`处理程序可以使用`applyCh`来发送快照，通过将快照放入到`ApplyMsg`中。service从`applyCh`中读取，然后和快照调用`CondInstallSnapshot`来告诉Raft：service正在切换成传入快照状态，然后Raft应该同时更新自己的日志。(参见`config.go`中的`applierSnap()`来查看tester服务是怎么做这个的)。

`CondInstallSnapshot`应该拒绝安装旧的快照(如果Raft在`lastIncludedTerm/lastIncludedIndex`之后处理了日志条目)。这个因为Raft可能在处理`InstallSnapshot`RPC之后和在service调用`CondInstallSnapshot`之前处理了其他的RPC并在`applyCh`上发消息了。不允许Raft回到旧的快照，因此必须拒绝旧的快照。当你的实现拒绝快照时，`CondInstallSnapshot`应该直接返回`false`使得service知道现在不应该切换这个快照。

如果快照是最近的，Raft应该调整日志，持久化新的状态，返回true，以及service应该切换到这个快照在处理下一个消息到`applyCh`之前。

`CondInstallSnapshot`是一种更新Raft和service的状态的方式；其他接口也是可以的。这个特殊的设计允许你的实现检查一个快照是否必须安装在一个地方，并且原子性地将service和Raft切换到快照。你可以自由地以`CondInstallSnapshot`始终可以返回`true`的方式来实现你的Raft。

#### Task

修改你的Raft代码来支持快照；实现`SnapShot`，`CondInstallSnapshot`以及`InstallSnapshot`的RPC。以及对Raft的更改来支持这些(例如：继续使用修剪后的日志)你的解决方案通过2D测试和所有的Lab2测试的时候，它就完成了。

#### Hints

1. 在一个`InstallSnapshot`中发送整个快照。不要实现图13的分割快照的`offset`机制。
2. Raft必须丢弃旧的日志以允许Go的垃圾回收器来使用和重用内存；这要求对于丢弃的日志没有可达的引用(指针)。
3. Raft的日志不再使用日志条目的位置或日志长度的方式来确定日志的条目索引；您需要使用独立于日志位置的索引方案。
4. 即使日志被修剪，你的实现仍需要在`AppendEntries`正确的发送日志的Term和Index；这可能需要保存和引用最新的快照的`lastIncludedTerm/lastIncludedIndex`(考虑是否应该持久化)
5. Raft必须存储每一个快照到persiser中，通过`SaveStateAndSnapshot()`。
6. 对于整套的Lab2测试，合理的时间消耗应该是8分钟的实际时间和1.5分钟的CPU时间。

---

## Lab3

### Introduction

在这个lab中，你将构建一个基于lab的Raft库实现的容错性kv存储服务。你的kv服务将是一个复制的状态机，由几个使用Raft进行复制的kv服务器组成。你的kv服务应该在当大多数服务器处于活动状态并且可以通信的时候继续处理客户机请求，即使出现其他故障或者网络分区。lab3之后，你将实现raft_diagram中的所有模块(Clerk，Service and Raft)。

这个服务支持三个操作：`Put(key, value)`，`Append(key, arg)`以及`Get(key)`。它维护一个简单的kv键值对数据库。键值对都是字符串。`Put()`替换数据库中特定键的值，`Append(key, arg)`将arg附加到key的值上，以及`Get()`获取当前键的值。`Get()`一个不存在的key将返回一个空字符串。`Append()`一个不存在的key就和`Put()`效果相同。每一个客户端和服务用一个`Clerk`的Put/Append/Get方法来交互。一个`Clerk`管理和服务器的RPC交互。

你的服务必须为`Clerk`的Get/Put/Append方法提供强一致性。以下是我们认为的强一致性。如果一次调用一个，那么Get/Put/Append方法应该像系统只有一个副本那样工作，每次调用都应该观察前面的调用序列所暗示的对状态的修改。对于并发调用，返回值和最终状态必须相同，就像操作以某种顺序一次执行一个操作一样。如果调用在时间上有重叠，例如，客户端X调用了`Clerk.Put()`，然后客户端Y调用`Clerk.Append()`，然后X的调用返回结果了。此外，一次调用必须观察在调用开始之前完成的所有调用的效果(所以我们在技术上要求线性化)。

强一致性对于应用程序来说很方便，因此这非正式地意味着，所有客户端看到的状态都是一样的并且看到了都是最新的状态。对于单机来说，提供强一致性相对容易。但是如果服务是有副本的，那就困难了。因为所有的服务器必须为并发请求选择相同的执行顺序，并且必须避免使用不是最新的状态来回复客户端。

这个lab有两个部分。在A部分，你将实现一个不用担心Raft的log会无限增长的服务。在B部分，你将实现一个快照(论文第7节)，也就是让Raft丢弃旧的log。

你应该重新阅读Raft论文，特别是第7和第8节。广阔来看，你可以看一下Chubby、Paxos Made Live、Spanner、Zookeeper、Harp、Viewstamped Replication以及Bolosky et al。

### Getting Started

我们在`src/kvraft`中提供框架代码和测试。你需要修改`kvraft/client.go`，`kvraft/server.go`以及可能也要`kvraft/common.go`

### Lab3a

> 一个没有快照的kv服务

每一个你的kv服务器都需要有一个与之关联的Raft节点。Clerks发送`Put()`，`Append()`和`Get()`的RPC请求到当前Raft中leader的那个kvserver上。kvserver将Put/Append/Get操作提交给Raft，以便Raft保存一系列Put/Append/Get操作。所有的kvserver从Raft的log中按顺序执行操作，并将这些操作应用到他们的kv数据库上。这样做的目的是在所有的服务器上维护相同的kv数据库副本。

一个`Clerk`有时候不知道哪一个kvserver是Raft的leader。如果一个`Clerk`发送一个RPC请求到错误的kvserver上，或者没有到达kvserver上；`Clerk`应该通过发送到不同的kvserver上来进行重试。如果一个kv服务将操作提交到它的Raft日志中(并将操作用用到kv状态机上)，那么leader将通过RPC来回应`Clerk`。如果操作没有成功提交(比如leader被更替了)，这个服务器将报告error，然后`Clerk`将在不同的服务器上重试。

你的kvservers不应该直接进行通信；它们应该只通过他们的Raft来进行通信。

#### Task1

你的首要任务是实现一个当没有消息丢失和服务器失败的解决方案。

你将需要增加`client.go`中关于Clerk的Put/Append/Get方法的RPC发送代码，以及实现`server.go`中的`PutAppend()`和`Get()`的RPC处理程序。这些处理程序应该使用`Start()`来在Raft日志中输入一个`Op`；你应该完善`server.go`里面的`Op`的结构定义以至于它可以描述一个Put/Append/Get操作。每一个服务器应该在Raft提交`Op`命令时执行这些命令，即当它们出现在`applyCh`时。RPC处理程序应该在Raft提交其`Op`时发出通知，然后回复RPC。

当你可靠的通过测试中的第一个测试："One Client"时，你就完成了这个任务。

#### Hints1

- 调用`Start()`之后，你的kvservers将需要等待Raft去完成agreement。已经达成一致的命令道道`applyCh`。你的代码需要保持读取`applyCh`当`PutAppend()`和`Get()`处理程序使用`Start()`提交了命令到Raft日志中的时候。注意kvserver和Raft库之间的死锁问题。
- 你可以向`ApplyMsg`和RPC处理程序比如说`AppendEntries`中添加字段。但是对于大多数实现都是不需要的。
- 一个kvserver不应该完成一个`Get()`的RPC请求当它不是大多数的那一部分。一个简单的解决方案就是在Raft日志中输入每一个`Get()`(以及每一个`Put()`和`Append()`)。你不必实现论文章第八节描述的只读操作的优化。
- 最好一开始就加锁因为避免死锁有时候会影响整个的代码设计。使用`go test -race`来检查的你的代码是不是race-free。

现在你需要修改你的解决方案，以便在网络和服务器出现故障时继续执行。您将面临的一个问题是，`Clerk`可能不得不的发送多次RPC知道它找到了一个积极响应的kvserver时。如果一个leader在向Raft日志中提交了一个条目之后失败了，`Clerk`可能不会收到回应，因此可能需要重发请求到别的leader那里。每次`Clerk.Put()`或者`Clerk.Append()`的调用应该最终只执行一次，因此你必须确保重发不会导致服务器执行两次请求。

#### Task2

添加代码来处理失败，并处理重复的`Clerk`请求，包括这样的情况：`Clerk`在一个任期内发送请求到kvserver的leader，等待回复超时，然后再另一个任期重新向新领导发送请求。请求应该只执行一次。您的代码应该能够通过

`go test -run 3A -race`。

#### Hints2

- 你的解决方案需要处理一个领导者，该领导者已经为Clerk的RPC请求调用了`Start()`，但是在将请求提交到日志之前失去了领导权。在这种情况，你应该安排`Clerk`去重发请求到其他服务器直到发到了新的leader那里。这样做的一个方法是，服务器通过注意`Start()`返回的索引出现了不同的请求，或者Raft的任期已经改变，来检测它是否失去了领导权。如果当前领导者自己分区，那么它不会知道新的领导者；但是同一分区的任何客户端也不能和新领导者交互；所以在这种情况下，服务器和客户端可以无限期的等待，直到分区恢复。
- 你可能必须修改您的Clerk，以便记住最后一个RPC的主机是哪个服务器，并首先将下一个RPC发送到该服务器。这将避免浪费时间在通过RPC来找领导者上，这可能会帮助你快速的通过一些测试。
- 你将需要唯一地标识客户端的操作，以确保kv服务只执行每个操作一次。
- 你的重复检测方案应该能够快速的释放服务器内存，例如，让暗示每一个RPC都看到了前一个RPC的回复。假设客户端只在Clerk调用一次是可以的。

