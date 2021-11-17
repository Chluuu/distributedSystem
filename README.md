# **Lab1：MapReduce**

1. ## **实验说明**

参照[MapReduce文章](http://research.google.com/archive/mapreduce-osdi04.pdf)实现对文件的高效并行处理，本实验按照[MIT 6.824 2020spring](http://nil.csail.mit.edu/6.824/2020/schedule.html)课程Lab1的各项要求完成，使用便于并发程序设计的golang作为开发语言，其中rpc相关功能依赖golang包含的net/rpc库实现。

1. ## **实现方法**

### **2.1 实验环境**

操作系统：Manjaro Linux

实现语言：Go 1.13

编译方式： go build

### **2.2 程序架构**

本实验通过完成master.go,worker.go,rpc.go三个文件实现了MapReduce框架，其中master.go文件主要包括Master Machine所要存储的数据结构和需要运行的方法，worker.go文件主要包括Worker Machine需要存储的数据和运行的方法，rpc.go主要存储Master Machine远程调用Master Machine的方法是需要的参数结构。Worker通过rpc方式远程调用Master方法，使这些方法在Master上运行，其中Reply参数要求是指针类型，这样在运行结束后Reply中会存储希望Master回馈的内容，三者关系如下图所示：

​                 ![img](https://docimg7.docs.qq.com/image/4ERnFWghOH2mxUw2f_Gyjw.png?w=746&h=464)        

在Master.go中定义Task结构，Master分配的任务和Worker需要执行的任务参数都包含在这个结构内

type Task struct{

​    Phase executePhase //表明此任务是Map还是Reduce

​    FileName string //需要执行Map任务的文件名称

​    Number int //Map或Reduce任务序号，用于中间文件和结果文件的生成

​    NReduce int //构建Master时传入的参数，表明整个MapReduce项目中Reduce子任务的个数

​    NMap int //与待处理文件数想对应，表明在程序运行过程中会有几个Map子任务

}

在Master.go中定义Master结构，包含了任务调度所需的所有参数

type Master struct {

​    // Your definitions here.

​    mutex sync.Mutex //防止下列元素发生Race状况的同步锁

​    Allfile []string //待处理的文件名

​    taskProcess []TaskProcess //当前所有Map任务或Reduce任务的状态

​    todoTask chan Task //Master先构建好待处理任务，再将这些任务全都放入管道中，等待Worker的执行

​    isDone bool //整个MapReduce过程是否执行完毕

​    projectPhase executePhase //整个项目处在的任务阶段（分为Map或Reduce阶段，只有Map全部结束后才会进入Reduce）

​    nReduce int //构建Master时传入的参数，表明整个MapReduce项目中Reduce子任务的个数

​    nMap int //与待处理文件数想对应，表明在程序运行过程中会有几个Map子任务

​    workernum int //Worker Machine数量

}

在Worker.go中定义Worker结构，包含了Worker执行任务所需的参数



type worker struct{

​    mapf func(string,string)[]KeyValue //.so文件中传入的map方法

​    reducef func(string,[]string)string //.so文件中传入的reduce方法

​    id int //Worker Machine在Master处的序列号

}

#### **Master调度过程**

在Master中通过定时调用Schedule方法实现任务的调度，用以下四个常量表明任务所处的阶段。

const(

​    TaskReady=0 

​    TaskQueue=1

​    TaskExecute=2

​    TaskFinish=3

​    TaskError=4 

)

简化调度过程如下图所示：

​                   ![img](https://docimg10.docs.qq.com/image/_OoZ0umJT7YvjFPB93Gaqg.png?w=593&h=212)        



### **2.3 实验执行方法**

实验基于[MIT 6.824 2020spring Lab1](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html)所提供的代码框架完成，在执行时首先在main目录下通过执行该指令进行编译，最后一个参数存储了mapf和reducef方法，本实验中提供了WordCount和Indexer两个任务。

go build -buildmode=plugin ../mrapps/wc.go

接着在一个窗口中启动Master，pg-*.txt是框架提供的待处理文件

go run mrmaster.go pg-*.txt

接着在多个窗口中启动Worker进程，so文件是第一步编译过程中得到的Mapf Reducef编译结果

go run mrworker.go wc.so

在我实现的Lab1中，当所有文件处理完毕后，Master和所有Worker进程都会自动结束。

## **3 实验结果**

通过构建的MapReduce项目执行Word Count任务，得到如下结果，与所给的非分布式程序处理结果一致。

执行结束后生成nReduce个结果文件

​                 ![img](https://docimg1.docs.qq.com/image/15kp1s-kKHGp71fLBYjU1A.png?w=1208&h=144)        

通过cat命令按顺序展示这些文件中的内容

​                 ![img](https://docimg3.docs.qq.com/image/eBXgNFP5hvehaEvjYk-wmA.png?w=1213&h=627)        

执行所给的测试脚本进行测试，主要查看是否能处理某个Worker Machine崩溃的情况，其中的rpc:Register报错信息不需要处理，是已知的警告信息，rpc希望这两个函数能有一定数量的参数，unexpected EOF是MapReduce任务处理完毕后Master自动结束，Worker请求任务失败并自动结束的信号，均属于正常输出

​                 ![img](https://docimg4.docs.qq.com/image/vGdSvph1HeM44DKuxdhJpQ.png?w=1236&h=360)        

测试结果显示通过了所有测试

​                 ![img](https://docimg9.docs.qq.com/image/CIwQjlnLVMVlUO1yraUkPw.png?w=1213&h=592)        

## **4 踩坑**

× go中所有和RPC调用相关的函数名以及参数名首字母必须大写，go的rpc库在到处这些值时会默认将首字母大写，如果原本文件中是小写那么在正式执行时会找不到这些方法或参数。

× 由于使用了多种同步方法，Channel的最简单用法（即不加容量）会使程序出现死锁，即当调度程序由于没有worker请求任务而阻塞时，由于sync.Mutex的使用，Worker无法启动并请求任务，这时可以使用有容量的Channel，容量大小取nReduce和nMap的最大值。

× 某些任务超时后有可能还会通过调用ReportTask返回结果，这时如果该任务已经完成，会由于对任务状态的不正确更新而出现错误，因此如果当调用参数的Phase和当前Master Phase不一致或调用参数的WorkerID和调用参数Index对应process的WorkerID不一致时不能对Process参数进行更新。

