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
	mapTasks         map[string]*Task
	reduceTasks      map[string]*Task
	nMapRemaining    int
	nReduceRemaining int
	nReduce          int
	lock             sync.Mutex
}

type Task struct {
	number   int
	status   TaskStatus
	workerId int
	file     string
	taskType TaskType
}

type TaskStatus string

const (
	unInitiated TaskStatus = "unInitiated"
	inProgress  TaskStatus = "inProgress"
	finished    TaskStatus = "finished"
)

const timeout = 10

func getAvailableTask(tasks map[string]*Task, workerId int) *Task {
	for _, v := range tasks {
		if v.status == unInitiated {
			v.status = inProgress
			v.workerId = workerId
			return v
		}
	}
	return &Task{taskType: exit}
}

func (c *Coordinator) ReplyWithTask(request *GetTaskRequest, reply *GetTaskResponse) error {
	c.lock.Lock()
	var task *Task

	if c.nMapRemaining > 0 {
		task = getAvailableTask(c.mapTasks, request.WorKerId)
	} else if c.nReduceRemaining > 0 {
		task = getAvailableTask(c.reduceTasks, request.WorKerId)
	} else {
		task = &Task{taskType: exit}
	}

	reply.FileName = task.file
	reply.TaskType = task.taskType
	reply.TaskNumber = task.number
	c.lock.Unlock()
	go c.WaitTask(task)
	return nil
}

func (c *Coordinator) WaitTask(task *Task) {
	// wait for timeout, if worker hasn't finished yet
	<-time.After(time.Second * timeout)
	// we will re-set task status to unInitiated
	c.lock.Lock()
	if task.taskType == exit {
		return
	}
	if task.status == inProgress {
		task.status = unInitiated
		task.workerId = -1
	}
	c.lock.Unlock()
}

func (c *Coordinator) TaskDone(request *CompletedTaskRequest, response *CompletedTaskResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	var task *Task
	var ok bool
	switch request.TaskType {
	case mapTask:
		task, ok = c.mapTasks[request.FileName]
		pullError(ok)
	case reduceTask:
		task, ok = c.reduceTasks[request.FileName]
		pullError(ok)
	default:
		log.Fatal("wrong task type") // worker should never send an exit taskType
	}

	if task.workerId == request.WorkerId && task.status == inProgress {
		task.status = finished
		if task.taskType == mapTask && c.nMapRemaining > 0 {
			fmt.Println("decrementing map tasks")
			c.nMapRemaining--
		} else if task.taskType == reduceTask && c.nReduceRemaining > 0 {
			fmt.Println("decrementing reduce tasks")
			c.nReduceRemaining--
		}
	}
	return nil
}

func pullError(ok bool) {
	if !ok {
		log.Fatal("file name is wrong")
	}
}

func (c *Coordinator) ReduceCount(request *ReduceCountRequest, response *ReduceCountResponse) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	response.ReduceCount = c.nReduce
	return nil
}

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

func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.nReduceRemaining == 0
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	var mapTasks = make(map[string]*Task)
	for i, file := range files {
		mapTasks[file] = &Task{
			number:   i,
			status:   unInitiated,
			taskType: mapTask,
			file:     file,
		}
	}

	var reduceTasks = make(map[string]*Task)
	for i := 0; i < nReduce; i++ {
		reduceTasks[strconv.Itoa(i)] = &Task{
			number:   i,
			status:   unInitiated,
			taskType: reduceTask,
		}
	}

	c := Coordinator{
		mapTasks:         mapTasks,
		reduceTasks:      reduceTasks,
		nMapRemaining:    len(mapTasks),
		nReduceRemaining: nReduce,
		nReduce:          nReduce,
		lock:             sync.Mutex{},
	}

	c.server()
	return &c
}
