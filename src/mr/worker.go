package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "encoding/json"
import "os"
//import "strings"

type worker struct{
	mapf func(string,string)[]KeyValue
	reducef func(string,[]string)string
	id int
}
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	w:=worker{}
	w.mapf=mapf
	w.reducef=reducef
	//fmt.Printf("worker initialize")
	Rargs:= &RegArgs{}
	Rreply:=&RegReply{}
	if ok:=call("Master.RegWorker",Rargs,Rreply);!ok{
		log.Fatal("reg fail")
	}
	w.id=Rreply.WorkerID

	for{
		Aargs:=&ReqTaskArgs{}
		Aargs.WorkerID=w.id
		Areply:=&ReqTaskReply{}
		//fmt.Printf("task assign")
		if ok:=call("Master.TaskAssign",Aargs,Areply);!ok{
			fmt.Printf("worker get task fail,exit")
			os.Exit(1)
		}
		t:=Areply.T
		w.doTask(t)
		//fmt.Printf("#finish %d",t.Number)

	}
	// uncomment to send the Example RPC to the master.
	// CallExample()

}
func shuffleName(mapidx int,redidx int) string {
	return fmt.Sprintf("mr-%d-%d", mapidx, redidx) 
}
func finaloutName(redidx int) string {
	return fmt.Sprintf("mr-out-%d", redidx)
}
func (w *worker)doTask(t *Task) {
	//fmt.Printf("in do Task")

	switch t.Phase {
	case MapPhase:
		w.doMapTask(t)
	case ReducePhase:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase err: %v", t.Phase))
	}
}
func (w *worker)doMapTask(t *Task) {
	//fmt.Printf("do map")
	file, err := os.Open(t.FileName)
	if err != nil {
		w.reportTask(*t, false, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		w.reportTask(*t, false, err)
		return
	}
	file.Close()
	kvs := w.mapf(t.FileName, string(content))
	shuffle:=make([][]KeyValue,t.NReduce)
	for _,kv:=range kvs{
		index:=ihash(kv.Key)%t.NReduce
		shuffle[index]=append(shuffle[index],kv)
	}
	for idx,shuffle_kv:=range shuffle{
		fileName:=shuffleName(t.Number,idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(*t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range shuffle_kv {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(*t, false, err)
			}

		}
		if err := f.Close(); err != nil {
			w.reportTask(*t, false, err)
		}

	}
	w.reportTask(*t,true,nil)


}
func(w *worker)doReduceTask(t *Task) {
	reduces:=make(map[string][]string)
	for i:=0;i<t.NMap;i++{
		fileName:=shuffleName(i,t.Number)
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(*t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for{
			var kv KeyValue
			if err:=dec.Decode(&kv);err!=nil{
				break
			}
	
			reduces[kv.Key]=append(reduces[kv.Key],kv.Value)
			
		}
		if erro := file.Close(); erro != nil {
			w.reportTask(*t, false, erro)
		}

	}
	ofile, _ := os.Create(finaloutName(t.Number))
	//outresult:=make([]string)
	for k,v:=range reduces{
		outresult:=w.reducef(k,v)
		fmt.Fprintf(ofile, "%v %v\n", k, outresult)
	}
	ofile.Close()
	w.reportTask(*t, true, nil)
}
func(w *worker)reportTask(t Task,Done bool,err error) {
	args:=ReportArgs{
		Done: Done,
		Index: t.Number,
		Phase: t.Phase,
		Workerid: w.id,
	}
	reply:=ReportReply{}
	if ok:=call("Master.ReportTask",&args,&reply);!ok{
		fmt.Sprintf("report fail:%+v",args)
	}
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
