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



