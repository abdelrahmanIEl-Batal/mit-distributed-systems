package mr

import "os"
import "strconv"

type GetTaskRequest struct {
	WorKerId int
}

type GetTaskResponse struct {
	FileName        string
	TaskNumber      int // used to refer to intermediate/output
	PartitionNumber int
	TaskType        TaskType
}

type CompletedTaskRequest struct {
	FileName string
	TaskType TaskType
	WorkerId int
}

type CompletedTaskResponse struct{}

type TaskType string

const (
	mapTask    TaskType = "mapTask"
	reduceTask TaskType = "reduceTask"
	exit       TaskType = "exit"
)

type ReduceCountRequest struct{}
type ReduceCountResponse struct {
	ReduceCount int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
