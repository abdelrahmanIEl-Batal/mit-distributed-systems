package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapTasks     map[string]*Task // will mark each file and its status
	reduceTasks  map[string]*Task
	nMapTasks    int
	nReduceTasks int
	lock         sync.Mutex
}

type Task struct {
	StartingTime time.Time
	Number       int
	Status       TaskStatus
	WorkerId     int
	TaskType     Type
	File         string
}

type TaskStatus string

const (
	unInitiated TaskStatus = "unInitiated"
	inProgress  TaskStatus = "inProgress"
	finished    TaskStatus = "finished"
)

const timeout = 10

// get uninitiatedTask
// these are all initial thoughts haven't tested anything
// we can just mark Task attributes with any marker, to know if there is no avail tasks
func getAvailableTask(tasks map[string]*Task, workerId int) *Task {
	for k, v := range tasks {
		if v.Status == unInitiated {
			v.Status = inProgress
			v.WorkerId = workerId
			v.File = k
			return v
		}
	}
	return &Task{Status: finished, WorkerId: -1, Number: -1}
}

// TODO think about concurrency and shared data, this is just a initial thoughts
// check if sometask is free and return it
func (c *Coordinator) ReplyWithTask(request *GetTaskRequest, reply *GetTaskResponse) error {
	// lock this block to avoid workers getting the same files ig, could be wrong
	c.lock.Lock()
	var task *Task
	if c.nMapTasks > 0 {
		task = getAvailableTask(c.mapTasks, request.WorKerId)
	} else if c.nReduceTasks > 0 {
		task = getAvailableTask(c.reduceTasks, request.WorKerId)
	} else {
		task = &Task{TaskType: exit}
	}

	reply.FileName = task.File
	reply.TaskType = task.TaskType
	reply.TaskNumber = task.Number
	defer c.lock.Unlock()
	// wait for task?, use go routing since we are waiting for multiple files not just one
	go c.WaitTask(task)
	// this should always return nil, meaning everything is ok, if returned error is not nil
	// then something is wrong with our logic ig?
	return nil
}

func (c *Coordinator) WaitTask(task *Task) {
	// wait for timeout, if worker hasn't finished yet
	<-time.After(time.Second * timeout)
	// we will re-assign this to another worker if not done during timeout
	if task.Status == inProgress {
		task.Status = unInitiated
		task.WorkerId = -1
	}
}

func (c *Coordinator) TaskDone(request *CompletedTaskRequest, response *CompletedTaskResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	var task *Task
	if request.TaskType == mapTask {
		pullTask, ok := c.mapTasks[request.FileName]
		if !ok {
			fmt.Println("file name is wrong")
			return nil
		}
		task = pullTask
	} else if request.TaskType == reduceTask {
		pullTask, ok := c.reduceTasks[request.FileName]
		if !ok {
			fmt.Println("file name is wrong")
			return nil
		}
		task = pullTask
	} else {
		fmt.Println("wrong task type")
		return nil
	}

	if task.WorkerId == request.WorkerId && task.Status == inProgress {
		task.Status = finished
		if task.TaskType == mapTask && c.nMapTasks > 0 {
			c.nMapTasks--
		} else if task.TaskType == reduceTask && c.nReduceTasks > 0 {
			c.nReduceTasks--
		}
	}
	return nil
}

func (c *Coordinator) ReduceCount(request *ReduceCountRequest, response *ReduceCountResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	response.ReduceCount = len(c.reduceTasks)
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
// need to handle when task gets re-assigned on timeout
func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.nReduceTasks == 0 && c.nMapTasks == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// we should have this initialise our mtasks, rtasks map
	var mapTasks = make(map[string]*Task)
	for i, file := range files {
		// will leave the starting time for now
		mapTasks[file] = &Task{
			Number:   i,
			Status:   unInitiated,
			TaskType: mapTask,
		}
	}

	var reduceTasks = make(map[string]*Task)
	for i := 0; i < nReduce; i++ {
		reduceTasks[strconv.Itoa(i)] = &Task{
			Number:   i,
			Status:   unInitiated,
			TaskType: reduceTask,
		}
	}

	c := Coordinator{
		mapTasks:     mapTasks,
		reduceTasks:  reduceTasks,
		nMapTasks:    len(mapTasks),
		nReduceTasks: nReduce,
		lock:         sync.Mutex{},
	}

	// Your code here.

	c.server()
	return &c
}
