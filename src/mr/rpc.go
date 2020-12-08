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
type RegArgs struct {
}

type RegReply struct {
	WorkerId int
}

type ReqTaskArgs struct {
	WorkerId	int
}

type ReqTaskReply struct {
	Task	*Task
}

type TaskArgs struct {
	WorkerId	int
}

type ReportTaskArgs struct {
	Done 		bool
	TaskId		int
	Phase 		TaskPhase
	WorkerId	int
}

type ReportTaskReply struct {

}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
