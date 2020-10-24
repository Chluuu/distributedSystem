package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "fmt"

type executePhase int

const(
	TaskReady=0
	TaskQueue=1
	TaskExecute=2
	TaskFinish=3
	TaskError=4	
)
const(
	MapPhase executePhase=0
	ReducePhase executePhase=1
)
const(
	TaskMaxFre=time.Second*10
	ScheduleFre =time.Millisecond*1000
)
type Task struct{
	Phase executePhase
	FileName string
	Number int
	NReduce int
	NMap int
}
type TaskProcess struct{
	StartT time.Time
	Process int
	WorkerID int
}
type Master struct {
	// Your definitions here.
	mutex sync.Mutex
	Allfile []string
	taskProcess []TaskProcess
	todoTask chan Task
	isDone bool
	projectPhase executePhase
	nReduce int
	nMap int
	workernum int
}
//
//init the master struct member,the master is during map phase originally
//
func (m *Master) init(){
	m.taskProcess=make([]TaskProcess,len(m.Allfile))
	m.isDone=false
	m.projectPhase=MapPhase
	m.nMap=len(m.Allfile)
}
// Your code here -- RPC handlers for the worker to call.
func (m *Master) TaskAssign(args *ReqTaskArgs,reply *ReqTaskReply) error { 
	task:=<-m.todoTask
	reply.T=&task
	m.mutex.Lock()
	defer m.mutex.Unlock()
	//fmt.Printf("task assign %d",task.Number)

	m.taskProcess[task.Number].Process=TaskExecute
	m.taskProcess[task.Number].WorkerID=args.WorkerID
	m.taskProcess[task.Number].StartT=time.Now()

	return nil
}

func (m *Master) RegWorker(args *RegArgs,reply *RegReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	reply.WorkerID=m.workernum
	m.workernum+=1
	//fmt.Printf("in regworker")
	return nil
}

func(m *Master) ReportTask(args *ReportArgs,reply *ReportReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.projectPhase != args.Phase || args.Workerid != m.taskProcess[args.Index].WorkerID {
		return nil
	}
	//fmt.Printf("#report task %d",args.Index)
	if args.Done{
		m.taskProcess[args.Index].Process=TaskFinish
	}else {
		m.taskProcess[args.Index].Process=TaskError
	}
	go m.Schedule();
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret=m.isDone

	return ret
}
//
//Create a task waiting to be executed
//
func(m*Master)getTask(index int)Task{
	t:=Task{}
	t.Phase=m.projectPhase
	if m.projectPhase==MapPhase {t.FileName=m.Allfile[index]}
	t.Number=index
	t.NReduce=m.nReduce
	t.NMap=m.nMap
	return t
}
//
//schdule tasks timely.
//
func (m*Master)Schedule(){
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.isDone{
		return
	}

	phaseFinish:=true	
	for index,tp:=range m.taskProcess{
		switch tp.Process{
		case TaskReady:
			phaseFinish=false
			tsk:=m.getTask(index)
			m.todoTask<-tsk
			//fmt.Printf("#schedule after send %d to channle with length %d",index,len(m.taskProcess))
			m.taskProcess[index].Process=TaskQueue
		case TaskQueue:
			//fmt.Printf("#queue %d",index)
			phaseFinish=false
		case TaskExecute:
			phaseFinish=false
			//fmt.Printf("#execute %d",index)
			if time.Now().Sub(tp.StartT)>TaskMaxFre {
				m.todoTask<-m.getTask(index)
				m.taskProcess[index].Process=TaskQueue
			}
		case TaskFinish:
		case TaskError:
			//fmt.Printf("#error %d",index)
			phaseFinish=false
			m.todoTask<-m.getTask(index)
			m.taskProcess[index].Process=TaskQueue
		}
	}
	if phaseFinish {
		if m.projectPhase==MapPhase{
			m.projectPhase=ReducePhase
			m.taskProcess=make([]TaskProcess,m.nReduce)
			//fmt.Printf("#change phase %d",len(m.taskProcess))
		} else {
			fmt.Printf("#All DONE")
			m.isDone=true
		}
	}
}

func (m*Master)regularSchedule(){
	for !m.Done(){
		m.Schedule()
		time.Sleep(ScheduleFre)
	}
}
//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	// Your code here.
	m.mutex=sync.Mutex{}
	//m.mutex_regworker=sync.Mutex{}
	m.nReduce=nReduce
	m.Allfile=files

	if nReduce>len(files) {
		m.todoTask=make(chan Task,nReduce)
	}else {m.todoTask=make(chan Task,len(files))}
	m.init()

	go m.regularSchedule()

	m.server()
	return &m
}
