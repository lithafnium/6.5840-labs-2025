package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	coordSockName = sockname

	for {
		task, err := RequestTask()
		if err != nil {
			fmt.Printf("Worker exiting")
			break
		}

		if task.MapTask != nil {
			RunMap(task.MapTask, mapf)
			CallTaskFinished(task.MapTask.Id, Map)
		} else if task.ReduceTask != nil {
			RunReduce(task.ReduceTask, reducef)
			CallTaskFinished(task.ReduceTask.Id, Reduce)
		} else {
			fmt.Println("No tasks available, sleeping")
			time.Sleep(time.Second)
		}

	}
}

func RunMap(mapTask *MapTask, mapf func(string, string) []KeyValue) {
	log.Println("Running Map with file", mapTask.Filename)
	filename := mapTask.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open file %v, %v", filename, err)
		return
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return
	}

	file.Close()

	kva := mapf(filename, string(content))

	filenames := make(map[string]string)

	// need to write the intermediate values - hash key by nreduce tasks
	for _, value := range kva {
		key := value.Key
		reduceId := ihash(key) % mapTask.NReduce

		filePath := strings.Join([]string{"mr-", strconv.Itoa(mapTask.MapId), "-", strconv.Itoa(reduceId), "-", "tmp"}, "")
		newFilePath := strings.Join([]string{"mr-", strconv.Itoa(mapTask.MapId), "-", strconv.Itoa(reduceId)}, "")
		file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
			return
		}

		enc := json.NewEncoder(file)
		err = enc.Encode(&value)
		if err != nil {
			log.Fatalf("failed to write to %v", filePath)
			return
		}

		filenames[filePath] = newFilePath
	}

	for tmpFileName, newFileName := range filenames {
		err := os.Rename(tmpFileName, newFileName)
		if err != nil {
			fmt.Println("Error renaming file:", err)
			return
		}
	}
}

func RunReduce(reduceTask *ReduceTask, reducef func(string, []string) string) {
	// read all the intermediate files
	// load into key value pairs
	// sort by key
	// get all the values for each key, call reduce

	log.Println("Running Reduce")

	oname := strings.Join([]string{"mr-out-", strconv.Itoa(reduceTask.ReduceId)}, "")
	ofile, _ := os.Create(oname)
	kva := []KeyValue{}

	for i := range reduceTask.NumMapTasks {
		filename := strings.Join([]string{"mr-", strconv.Itoa(i), "-", strconv.Itoa(reduceTask.ReduceId)}, "")
		file, err := os.Open(filename)
		if err != nil {
			log.Println("cannot open file", filename, err)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

	}

	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	ofile.Close()

}

func CallTaskFinished(taskId string, taskType TaskType) {
	args := TaskFinishedArgs{}

	if taskType == Map {
		args.MapTaskId = &taskId
	} else {
		args.ReduceTaskId = &taskId
	}

	reply := RequestTaskReply{}

	ok := call("Coordinator.TaskFinished", &args, &reply)

	if ok {
		// todo
	} else {
		fmt.Printf("CallTaskFinished failed!\n")
	}

}

func RequestTask() (RequestTaskReply, error) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)

	if ok {
		return reply, nil
	} else {
		fmt.Printf("call failed!\n")
		return reply, errors.New("Request failed")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
