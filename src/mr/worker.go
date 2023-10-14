package mr

import (
	"encoding/json"
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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		taskResponse, ok, workerId := RequestTask()
		// sth happened connecting to coordinator
		if ok == false {
			return
		}
		// no more tasks we exit
		if taskResponse.TaskType == exit {
			fmt.Printf("no more tasks available, worker exiting ...")
			return
		}
		if taskResponse.TaskType == mapTask {
			// do map here then call done when done
			fmt.Printf("starting map task ...")
			DoMapWork(taskResponse, mapf, workerId)
		} else if taskResponse.TaskType == reduceTask {
			fmt.Printf("starting reduce task ...")
			DoReduceWork(reducef, taskResponse.TaskNumber, workerId)
		}
		// wait for 10 sec before asking for another request, we can tune this to be higher/lower
		<-time.After(time.Second * 2)
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
		file, err := os.Create(filepath.Join("map-results", filename))
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

func DoReduceWork(reducef func(string, []string) string, reduceId int, workerId int) {
	// matches all files with reduceId
	files, err := filepath.Glob(fmt.Sprintf("%v/mr-%v-%v", "map-results", "*", reduceId))
	if err != nil {
		fmt.Printf("error fetting map result files names")
		return
	}
	keyValueMap := make(map[string][]string)
	tmp := KeyValue{}
	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Printf("error opening file")
			return
		}
		content := json.NewDecoder(file)
		for content.More() {
			err := content.Decode(&tmp)
			if err != nil {
				fmt.Printf("error reading from file")
				return
			}
			keyValueMap[tmp.Key] = append(keyValueMap[tmp.Key], tmp.Value)
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
			fmt.Printf("worker finished successfully")
			return
		} else {
			fmt.Printf("worker failed")
			return
		}
	}
}

func writeReduceOutputToFiles(reducef func(string, []string) string, keyValueMap map[string][]string, reduceId int) {
	keys := make([]string, 0, len(keyValueMap))
	for k := range keyValueMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Create temp file
	filePath := fmt.Sprintf("%v/mr-out-%v-%v", "reduce-results", reduceId, os.Getpid())
	file, err := os.Create(filePath)
	if err != nil {
		fmt.Printf("error creating files")
		return
	}
	// Call reduce and write to temp file
	for _, k := range keys {
		v := reducef(k, keyValueMap[k])
		_, err := fmt.Fprintf(file, "%v %v\n", k, v)
		if err != nil {
			fmt.Printf("error reducing result")
			return
		}
	}

	// atomically rename temp files to ensure no one observes partial files
	file.Close()
	newPath := fmt.Sprintf("mr-out-%v", reduceId)
	err = os.Rename(filePath, newPath)
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
