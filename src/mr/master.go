package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus int
type TaskPhase int

const (
	TaskStatusReady TaskStatus = iota
	TaskStatusQueue
	TaskStatusRunning
	TaskStatusFinished
	TaskStatusErr
)

const (
	MaxTaskRunTime		= time.Second * 5
	ScheduleInterval	= time.Millisecond * 500
)

const (
	MapPhase TaskPhase = 0
	ReducePhase TaskPhase = 1
)

type Task struct {
	FileName string
	NReduce  int
	NMap     int
	TaskId   int
	Phase    TaskPhase
	Alive    bool
}

type TaskStat struct {
	Status	TaskStatus
	workId	int
	startTime time.Time
}

type Master struct {
	// Your definitions here.
	files		[]string
	nReduce		int
	taskPhase 	TaskPhase
	taskStats	[]TaskStat
	mu			sync.Mutex
	nWorkers	int
	done		bool
	taskCh		chan Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

const DISPLAY = false

func display(str string) {
	if DISPLAY {
		fmt.Println(str)
	}
}

// schedule task
func (m *Master) schedule() {
	//fmt.Println("Processing ...")

	// check all task status
	flag := true

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}

	for id, taskStat := range m.taskStats {
		//fmt.Printf("Task %d : %v", id, taskStat.Status)
		switch taskStat.Status {
		case TaskStatusReady:
			flag = false
			m.addTask(id)
		case TaskStatusQueue:
			flag = false
		case TaskStatusRunning:
			flag = false
			m.checkBreak(id)
		case TaskStatusFinished:
		case TaskStatusErr:
			flag = false
			m.addTask(id)
		default:
			panic("Tasks status schedule error...")
		}
	}

	if flag {
		if m.taskPhase == MapPhase {
			m.initReduceTasks()
		} else {
			m.done = true
		}
	}
}

func (m *Master) tickSchedule() {
	for !m.Done() {
		//fmt.Println("loop...")
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

// init map task
func (m *Master) initMapTasks()  {
	m.taskPhase = MapPhase
	m.taskStats = make([]TaskStat, len(m.files))
	display("Map task init...")
}

func (m *Master) initReduceTasks()  {
	m.taskPhase = ReducePhase
	m.taskStats = make([]TaskStat, m.nReduce)
	display("Reduce task init...")
}

// Register Worker
func (m *Master) RegWorker(args *RegArgs, reply *RegReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	display("register worker........")
	m.nWorkers += 1
	reply.WorkerId = m.nWorkers

	return nil
}

// Register task
func (m *Master) RegTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.taskStats[task.TaskId].Status = TaskStatusRunning
	m.taskStats[task.TaskId].workId = args.WorkerId
	m.taskStats[task.TaskId].startTime = time.Now()
}

// Request a task
func (m *Master) ReqTask(args *TaskArgs, reply *ReqTaskReply) error {
	task := <-m.taskCh
	reply.Task = &task
	display("request task...")
	if task.Alive {
		m.RegTask(args, &task)
	}

	display(fmt.Sprintf("get a task, args:%+v, reply:%+v\n", *args, *reply))
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if args.Done {
		m.taskStats[args.TaskId].Status = TaskStatusFinished
	} else {
		m.taskStats[args.TaskId].Status = TaskStatusErr
	}

	go m.schedule()

	return nil
}

func (m *Master) checkBreak(taskId int) {
	if time.Now().Sub(m.taskStats[taskId].startTime) > MaxTaskRunTime {
		m.addTask(taskId)
	}
}

func (m *Master) addTask(taskId int)  {
	m.taskStats[taskId].Status = TaskStatusQueue
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMap:     len(m.files),
		TaskId:   taskId,
		Phase:    m.taskPhase,
		Alive:    true,
	}

	if m.taskPhase == MapPhase {
		task.FileName = m.files[taskId]
	}

	m.taskCh <- task
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	// Your code here.
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// NReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.files = files
	
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(m.files))
	}

	m.initMapTasks()
	go m.tickSchedule()

	m.server()
	display("master init...")
	return &m
}