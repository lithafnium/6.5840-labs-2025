package mr

import (
	"time"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	Map TaskType = iota
	Reduce
)

type TaskStatus int

const (
	Running TaskStatus = iota
	Pending
	Finished
)

type RequestTaskArgs struct {
	// args worker sends to coordinator
}

type RequestTaskReply struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type TaskFinishedArgs struct {
	MapTaskId    *string
	ReduceTaskId *string
}

type TaskFinishedReply struct{}

type Task interface {
	send() error
}

type MapTask struct {
	Id            string
	TaskType      TaskType
	Status        TaskStatus
	Filename      string
	NReduce       int
	MapId         int // file index
	RequestedTime *time.Time
}

func (t *MapTask) send() {
	now := time.Now()
	t.Status = Running
	t.RequestedTime = &now
}

type ReduceTask struct {
	Id            string
	TaskType      TaskType
	Status        TaskStatus
	ReduceId      int
	NumMapTasks   int
	RequestedTime *time.Time
}

func (t *ReduceTask) send() {
	now := time.Now()
	t.Status = Running
	t.RequestedTime = &now
}
