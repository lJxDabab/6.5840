package mr

import "fmt"
import "io/ioutil"
import "encoding/json"
import "os"
import "log"
// import "path/filepath"
import "net/rpc"
import "hash/fnv"
// import "time"
import "sort"
import "strconv"
// import "sort"
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type BKey []KeyValue

// for sorting by key.
func (a BKey) Len() int           { return len(a) }
func (a BKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValues struct{
	Key string
	Values []string
}

type BKeys []KeyValues

func (a BKeys) Len() int           { return len(a) }
func (a BKeys) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a BKeys) Less(i, j int) bool { return a[i].Key < a[j].Key }
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
	reply:=MapReply{}
	reply.Finished=false
	for true{
	reply=reqMap()
	if reply.Finished==false {
	filename:=reply.Filename
	WorkID:=reply.WorkID
	data,_:=ioutil.ReadFile(filename)
	kva:=mapf(filename,string(data))
	mediulname:=""
	exkvs:=make(map[int][]KeyValue)
	for i:=0;i<len(kva);i++{
		idx:=ihash(kva[i].Key)%reply.NReduce
		exkvs[idx]=append(exkvs[idx],kva[i])
	}
	for idx:=0;idx<reply.NReduce;idx++{
		mediulname="mr-"+strconv.Itoa(WorkID)+"-"+strconv.Itoa(idx)+".json"
		mediulfile,_:=os.OpenFile(mediulname,os.O_CREATE|os.O_APPEND|os.O_WRONLY,0644)
		encoder:=json.NewEncoder(mediulfile)
		encoder.Encode(exkvs[idx])
		mediulfile.Close()
		mapReturn(WorkID)
	}
}else{
	break
}
	}
	var Rfinished bool =false
	replyR:=ReduceReply{}
	replyR.Finished=false
	for reply.Finished==true && Rfinished==false{
		replyR=reqReduce()
		if replyR.Finished==true{
			break
		}
		container:=[]KeyValue{}
		exkv:=[]KeyValue{}
				for i:=0;i<8;i++{
					// fmt.Printf("id:%v,Name:%v\n",stringSufix,info.Name())
					exfile,_:=os.Open("mr-"+strconv.Itoa(i)+"-"+strconv.Itoa(replyR.ReduceID)+".json")
					encoder:=json.NewDecoder(exfile)
					encoder.Decode(&exkv)
					exfile.Close()
					container=append(container,exkv...)
				}
		sort.Sort(BKey(container))
		containerNew:=[]KeyValues{}
		strList:=[]string{}
		for idx,_:=range container{
			if idx==0{
				strList=append(strList,container[0].Value)
				continue
			}
			if container[idx].Key!=container[idx-1].Key {
				containerNew=append(containerNew,KeyValues{container[idx-1].Key,strList})
				strList=[]string{container[idx].Value}
				if idx==(len(container)-1){
					containerNew=append(containerNew,KeyValues{container[idx].Key,strList})
				}
				continue
			}
			strList=append(strList,container[idx].Value)
			if idx==(len(container)-1){
				containerNew=append(containerNew,KeyValues{container[idx].Key,strList})
			}
		}
		strs:=""
		for _,ele:=range containerNew{
			strs+=fmt.Sprintf("%v %v\n",ele.Key,reducef(ele.Key,ele.Values))
		}
		file,_:=os.OpenFile("mr-out-"+strconv.Itoa(replyR.ReduceID),os.O_CREATE|os.O_RDWR,0644)
		file.WriteString(strs)
		file.Close()
		Rfinished=ReduceReturn(replyR.ReduceID)
	}

}
func ReduceReturn(ReduceID int) bool{
	args:=ReduceReply{}
	args.ReduceID=ReduceID
	reply:=MapReply{}
	call("Coordinator.RPC_handler_RR", &args, &reply)
	return reply.Finished
}
func reqReduce() ReduceReply{
	args:=ReduceArgs{}
	reply:=ReduceReply{}
	call("Coordinator.RPC_handler_R",&args,&reply)
	return reply
}

func mapReturn(idx int) MapReply{
		args:=MapRetArgs{}
		args.WorkID=idx
		reply:=MapReply{}
		call("Coordinator.RPC_handler_MR", &args, &reply)
		return reply
}

func reqMap() MapReply{

	// declare an argument structure.
	args := MapArgs{}

	// fill in the argument(s).
	args.Type =0

	// declare a reply structure.
	reply := MapReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.RPC_handler", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.filename %v\n", reply.Filename)
		return reply
	} else {
		fmt.Printf("call failed!\n")
		return reply
	}
}
//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
