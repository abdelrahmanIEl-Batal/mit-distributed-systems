package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
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

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// infinite loop to keep requesting for more tasks
	for {
		taskResponse, workerId, err := RequestTask()
		haltWorker(err)
		if taskResponse.TaskType == mapTask {
			//fmt.Println("starting map task ...")
			DoMapWork(taskResponse, mapf, workerId)
		} else if taskResponse.TaskType == reduceTask {
			//fmt.Println("starting reduce task ...")
			DoReduceWork(reducef, taskResponse.TaskNumber, workerId)
		} else {
			fmt.Println("no more tasks available, worker exiting ...")
			return
		}
		<-time.After(time.Second * 1)
	}
}

func DoMapWork(reply *GetTaskResponse, mapf func(string, string) []KeyValue, workerId int) {

	file, err := os.Open(reply.FileName)
	haltWorker(err)

	content, err := io.ReadAll(file)
	haltWorker(err)

	err = file.Close()
	haltWorker(err)

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
	reduces := make(map[int][]KeyValue)
	for _, res := range data {
		idx := ihash(res.Key) % reduceCount
		reduces[idx] = append(reduces[idx], res)
	}

	for partitionNum, mapData := range reduces {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskNumber, partitionNum)
		file, err := os.Create(filename)
		haltWorker(err)
		enc := json.NewEncoder(file)
		for _, element := range mapData {
			err := enc.Encode(&element)
			haltWorker(err)
		}
		err = file.Close()
		haltWorker(err)
	}

	taskDoneReq := CompletedTaskRequest{
		FileName: mapFileName,
		WorkerId: workerId,
		TaskType: mapTask,
	}
	taskDoneRes := CompletedTaskResponse{}

	ok := call("Coordinator.TaskDone", &taskDoneReq, &taskDoneRes)
	if ok {
		fmt.Println("map task finished successfully")
	} else {
		fmt.Println("map task failed")
	}
}

func DoReduceWork(reducef func(string, []string) string, reduceId int, workerId int) {

	files, err := filepath.Glob(fmt.Sprintf("mr-%v-%v", "*", reduceId))
	haltWorker(err)

	keyValueMap := make(map[string][]string)
	tmp := KeyValue{}
	for _, fileName := range files {
		file, err := os.Open(fileName)
		haltWorker(err)

		content := json.NewDecoder(file)
		for content.More() {
			err := content.Decode(&tmp)
			haltWorker(err)
			keyValueMap[tmp.Key] = append(keyValueMap[tmp.Key], tmp.Value)
		}
	}

	writeReduceOutputToFiles(reducef, keyValueMap, reduceId)

	taskDoneReq := CompletedTaskRequest{
		FileName: strconv.Itoa(reduceId),
		WorkerId: workerId,
		TaskType: reduceTask,
	}
	taskDoneRes := CompletedTaskResponse{}
	ok := call("Coordinator.TaskDone", &taskDoneReq, &taskDoneRes)
	if ok {
		fmt.Println("worker finished successfully")
		return
	} else {
		fmt.Println("worker failed")
		return
	}
}

func writeReduceOutputToFiles(reducef func(string, []string) string, keyValueMap map[string][]string, reduceId int) {
	keys := make([]string, 0, len(keyValueMap))
	for k := range keyValueMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	filePath := fmt.Sprintf("mr-out-%v", reduceId)
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	haltWorker(err)
	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, keyValueMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, v)
		haltWorker(err)
	}

	file.Close()
}

func RequestTask() (*GetTaskResponse, int, error) {
	args := GetTaskRequest{
		WorKerId: os.Getpid(), // get id of process running (worker) as id
	}
	reply := GetTaskResponse{}
	ok := call("Coordinator.ReplyWithTask", &args, &reply)
	if ok {
		return &reply, args.WorKerId, nil
	} else {
		return nil, 0, errors.New("error requesting task")
	}
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
	return false
}

func haltWorker(e error) {
	if e != nil {
		log.Fatal("error executing worker: ", e)
	}
}
