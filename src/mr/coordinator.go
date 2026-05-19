package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    map[string]MapTask
	reduceTasks map[string]ReduceTask
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	now := time.Now()
	if len(c.mapTasks) == 0 {
		for id, task := range c.reduceTasks {
			if task.Status == Pending {
				task.send()
				c.reduceTasks[id] = task
				reply.ReduceTask = &task
				break
			} else if task.Status == Running && task.RequestedTime != nil && now.Sub(*task.RequestedTime).Seconds() > 10 {
				task.send()
				c.reduceTasks[id] = task
				reply.ReduceTask = &task
				break
			}
		}
	} else {
		for id, task := range c.mapTasks {
			if task.Status == Pending {
				task.send()
				c.mapTasks[id] = task
				reply.MapTask = &task
				break
			} else if task.Status == Running && task.RequestedTime != nil && now.Sub(*task.RequestedTime).Seconds() > 10 {
				task.send()
				c.mapTasks[id] = task
				reply.MapTask = &task
				break
			}
		}
	}
	defer c.mu.Unlock()
	return nil
}

func (c *Coordinator) TaskFinished(args *TaskFinishedArgs, reply *TaskFinishedReply) error {
	c.mu.Lock()
	if args.MapTaskId != nil {
		log.Println("Marking task", *args.MapTaskId, "as done")
		delete(c.mapTasks, *args.MapTaskId)
	} else if args.ReduceTaskId != nil {
		log.Println("Marking task", *args.ReduceTaskId, "as done")
		delete(c.reduceTasks, *args.ReduceTaskId)
	} else {
		return errors.New("Invalid task finished call")
	}

	defer c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	done := len(c.mapTasks) == 0 && len(c.reduceTasks) == 0

	log.Println("coordinator done:", done)
	defer c.mu.Unlock()
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	mapTasks := make(map[string]MapTask)
	reduceTasks := make(map[string]ReduceTask)
	numFiles := len(files)
	for i, file := range files {
		mapTasks[file] = MapTask{
			Id:       file,
			TaskType: Map,
			Status:   Pending,
			Filename: file,
			NReduce:  nReduce,
			MapId:    i,
		}

	}

	for i := range nReduce {
		reduceId := strconv.Itoa(i)
		reduceTasks[reduceId] = ReduceTask{
			Id:          reduceId,
			TaskType:    Reduce,
			Status:      Pending,
			ReduceId:    i,
			NumMapTasks: numFiles,
		}
	}

	c.mapTasks = mapTasks
	c.reduceTasks = reduceTasks
	c.server(sockname)
	return &c
}
