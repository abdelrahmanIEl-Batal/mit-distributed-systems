package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

// First Request Messages
type GetTaskRequest struct {
	WorKerId int
}

type GetTaskResponse struct {
	FileName   string
	TaskNumber int // used to refer to intermediate/output
	TaskType   Type
}

type CompletedTaskRequest struct {
	FileName string
	TaskType Type
	WorkerId int
}

type CompletedTaskResponse struct{}

type Type string

const (
	mapTask    Type = "mapTask"
	reduceTask Type = "reduceTask"
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
