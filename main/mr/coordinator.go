package mr

import "log"
import "net"
import "os"
import "fmt"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type ayufile struct{
	Filename string
}

type Coordinator struct {
	// Your definitions here.
	ayufiles_doing_M [8]ayufile
	ayufiles_undo_M [8]ayufile
	ayufiles_doing_R []bool
	ayufiles_undo_R []bool
	ayulock sync.Mutex
	nReduce int
	Finished bool
	IDgifter int
	IDgifter_R int 
}
// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RPC_handler(args *MapArgs, reply *MapReply) error{
	reply.NReduce=c.nReduce
	c.ayulock.Lock()
	fmt.Print("")
	for idx,file:=range c.ayufiles_undo_M{
		if file!=(ayufile{}) {
			reply.WorkID=idx
			reply.Filename=file.Filename
			c.ayufiles_undo_M[idx]=ayufile{}
			c.ayufiles_doing_M[idx]=file
			go c.Mapwait(reply.WorkID)
			c.ayulock.Unlock()
			return nil
		}
}
for _,file:=range c.ayufiles_doing_M{
	if file!=(ayufile{}) {
		c.ayulock.Unlock()
		time.Sleep(5*time.Millisecond)
		return nil
	}
}
	reply.Finished=true
	c.ayulock.Unlock()
	return nil
}
func (c *Coordinator)Mapwait(id int){
	time.Sleep(10*time.Second)
	c.ayulock.Lock()
	if c.ayufiles_doing_M[id]!=(ayufile{}){
		c.ayufiles_undo_M[id]=c.ayufiles_doing_M[id]
		c.ayufiles_doing_M[id]=ayufile{}
	}
	c.ayulock.Unlock()
}
func (c *Coordinator) RPC_handler_R(args *ReduceArgs, reply *ReduceReply) error{
	c.ayulock.Lock()
	for idx,_:=range c.ayufiles_undo_R{
		if c.ayufiles_undo_R[idx]==true{
			reply.ReduceID=idx
			c.ayufiles_undo_R[idx]=false
			c.ayufiles_doing_R[idx]=true
			go c.Reducewait(reply.ReduceID)
			c.ayulock.Unlock()
			return nil
		}
}
	for idx,_:=range c.ayufiles_doing_R{
		if c.ayufiles_doing_R[idx]==true{
			c.ayulock.Unlock()
			time.Sleep(5*time.Millisecond)
			return nil
		}
	}
	reply.Finished=true
	c.ayulock.Unlock()
	return nil
}
func (c *Coordinator)Reducewait(id int){
	time.Sleep(10*time.Second)
	c.ayulock.Lock()
	if c.ayufiles_doing_R[id]==true{
		c.ayufiles_undo_R[id]=true
		c.ayufiles_doing_R[id]=false
	}
	c.ayulock.Unlock()
}
func (c *Coordinator) RPC_handler_RR(args *ReduceReply, reply *MapReply) error{
	c.ayulock.Lock()
	reply.Finished=false
	WorkID:=args.ReduceID
	c.ayufiles_doing_R[WorkID]=false
	for _,file:=range c.ayufiles_undo_R{
		if file==true{
			c.ayulock.Unlock()
			return nil
		}
	}
	for _,file:=range c.ayufiles_doing_R{
		if file==true{
			c.ayulock.Unlock()
			return nil
		}
	}
		reply.Finished=true
	c.ayulock.Unlock()
	return nil
}
func (c *Coordinator) RPC_handler_MR(args *MapRetArgs, reply *MapReply) error{
	WorkID:=args.WorkID
	c.ayulock.Lock()
	c.ayufiles_doing_M[WorkID]=ayufile{}
	c.ayulock.Unlock()
	return nil
}
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	if c.IDgifter_R==c.nReduce{
		ret=true
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	for idx,filename:=range files {
		c.ayufiles_undo_M[idx].Filename=filename
	}
	for i:=0;i<nReduce;i++{
		c.ayufiles_doing_R=append(c.ayufiles_doing_R,false)
		c.ayufiles_undo_R=append(c.ayufiles_undo_R,true)
	}
	c.nReduce=nReduce
	c.Finished=false
	c.IDgifter=0
	c.IDgifter_R=0
	c.server()
	return &c
}
