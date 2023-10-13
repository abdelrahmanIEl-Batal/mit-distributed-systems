package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	taskResponse, ok, workerId := RequestTask()
	// sth happened connecting to coordinator
	if !ok {
		return
	}
	// no more tasks we exit
	if taskResponse.TaskType == exit {
		return
	}
	if taskResponse.TaskType == mapTask {
		// do map here then call done when done
		fmt.Printf("starting task")
		DoMapWork(taskResponse, mapf, workerId)
	} else if taskResponse.TaskType == reduceTask {
		// do reduce
	}

}

func DoMapWork(reply *GetTaskResponse, mapf func(string, string) []KeyValue, workerId int) {

	file, err := os.Open(reply.FileName)
	if err != nil {
		fmt.Printf("error opening file")
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		fmt.Printf("error reading file content")
		return
	}
	err = file.Close()
	if err != nil {
		fmt.Printf("error closing file")
	}
	result := mapf(reply.FileName, string(content))

	request := ReduceCountRequest{}
	response := ReduceCountResponse{}
	ok := call("Coordinator.ReduceCount", &request, &response)
	if ok {
		writeMapOutputToFiles(response.ReduceCount, result, reply.TaskNumber, reply.FileName, workerId)
	} else {
		println("failed to get reduce count")
	}
}

func writeMapOutputToFiles(reduceCount int, data []KeyValue, mapTaskNumber int, mapFileName string, workerId int) {
	reduces := make([][]KeyValue, reduceCount)
	for _, res := range data {
		idx := ihash(res.Key) % reduceCount
		reduces[idx] = append(reduces[idx], res)
	}
	for idx, mapData := range reduces {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskNumber, idx)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Printf("error creating file")
			return
		}
		// to easily read and write from files
		enc := json.NewEncoder(file)
		for _, element := range mapData {
			err := enc.Encode(&element)
			if err != nil {
				fmt.Printf("error writing to file")
				return
			}
		}
		err = file.Close()
		if err != nil {
			fmt.Printf("error closing file")
			return
		}
	}
	taskDoneReq := CompletedTaskRequest{
		FileName: mapFileName,
		WorkerId: workerId,
		TaskType: mapTask,
	}
	taskDoneRes := CompletedTaskResponse{}
	ok := call("Coordinator.TaskDone", &taskDoneReq, &taskDoneRes)
	if ok {
		fmt.Printf("worker finished successfully")
	} else {
		fmt.Printf("worker failed")
	}
}

func RequestTask() (*GetTaskResponse, bool, int) {
	args := GetTaskRequest{
		WorKerId: os.Getpid(), // get id of process running (worker) as id
	}
	reply := GetTaskResponse{}
	ok := call("Coordinator.ReplyWithTask", &args, &reply)
	return &reply, ok, args.WorKerId
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

	fmt.Println(err, "haha")
	return false
}
