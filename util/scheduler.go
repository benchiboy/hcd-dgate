// util
package util

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/astaxie/beego/logs"
)

//SECOND
const CHECK_INTERVAL = 1
const MAX_TICKS = 100

/*
	YYYY-MM-DD-W-HH-MM-SS
	YYYY-MM-DD-1-05-MM-SS
	YYYY-MM-DD-1-08-MM-SS
	YYYY-MM-DD-W-05-MM-SS
*/

type Schedule struct {
	TickChs  chan time.Time
	StopTick chan struct{}
	add      chan *Task_Info
	Tasks    *Task_List
}

type Task_List struct {
	Lock *sync.Mutex
	List []Task_Info
}

type Task_Info struct {
	Task_Id    int
	Task_Desc  string
	Calendar   string
	RJob       Job
	IsParellel bool
	IsRuning   bool
	next_time  time.Time
	prev_time  time.Time
}

type Job interface {
	Run(taksId int, callBack func(para int))
}

/*
	函数类型实现接口的Run
	为了实现非并发执行任务，任务执行完毕之后，需要回调传入的FUNC
	在FUNC 里面可以设置任务为结束状态
*/
type JobFunc func(taskId int)

func (j JobFunc) Run(taksId int, callBack func(para int)) {
	j(taksId)
	if callBack != nil {
		callBack(taksId)
	}
}

/*
	Schedule constructor
*/
func New_Schdule() *Schedule {
	tick_chs := make(chan time.Time, MAX_TICKS)
	stopTick_chs := make(chan struct{})
	return &Schedule{TickChs: tick_chs, StopTick: stopTick_chs, Tasks: &Task_List{Lock: &sync.Mutex{}}}
}

/*
	Add Task
*/
func (y *Schedule) Add_Task(taskId int, task_desc string, isParellel bool, calendar string, j Job) int {
	t := (Task_Info{Task_Id: taskId, Task_Desc: task_desc, IsParellel: isParellel, Calendar: calendar, RJob: j})
	y.Tasks.Lock.Lock()
	y.Tasks.List = append(y.Tasks.List, t)
	y.Tasks.Lock.Unlock()
	return len(y.Tasks.List)
}

/*
	Add Task
*/
func (y *Schedule) Add_TaskFunc(taskId int, task_desc string, isParellel bool, calendar string, f func(para int)) int {
	t := (Task_Info{Task_Id: taskId, Task_Desc: task_desc, IsParellel: isParellel, Calendar: calendar, RJob: JobFunc(f)})
	y.Tasks.Lock.Lock()
	y.Tasks.List = append(y.Tasks.List, t)
	y.Tasks.Lock.Unlock()
	return len(y.Tasks.List)
}

/*
	Add Task
*/
func (y *Schedule) Add_TaskFunc2(taskId int, task_desc string, isParellel bool, calendar string, f func(para int)) {
	t := (Task_Info{Task_Id: taskId, Task_Desc: task_desc, IsParellel: isParellel, Calendar: calendar, RJob: JobFunc(f)})
	y.add <- &t
	return
}

/*
	del task by task_id
*/

func (y *Schedule) Del_Task(task_id int) int {
	var index int
	for i, v := range y.Tasks.List {
		if v.Task_Id == task_id {
			index = i
			break
		}
	}
	y.Tasks.Lock.Lock()
	y.Tasks.List = append(y.Tasks.List[:index], y.Tasks.List[index+1:]...)
	y.Tasks.Lock.Unlock()
	return len(y.Tasks.List)
}

/*
	del task by task_id
*/

func (y *Schedule) Del_AllTask() {
	y.Tasks.Lock.Lock()
	y.Tasks.List = nil
	y.Tasks.Lock.Unlock()
}

/*
	get task by task_id
*/
func (y *Schedule) Get_Task(task_id int) Task_Info {
	var index int
	for i, v := range y.Tasks.List {
		if v.Task_Id == task_id {
			index = i
			break
		}
	}
	return y.Tasks.List[index]
}

/*
	get task by task_id
*/
func (y *Schedule) Set_TaskState(task_id int, state bool) error {
	var index int
	for i, v := range y.Tasks.List {
		if v.Task_Id == task_id {
			index = i
			break
		}
	}
	y.Tasks.List[index].IsRuning = state
	logs.Info("设置任务状态", state)
	return nil
}

/*
	get task by task_id
*/
func (y *Schedule) Get_TaskState(task_id int) bool {
	var index int
	for i, v := range y.Tasks.List {
		if v.Task_Id == task_id {
			index = i
			break
		}
	}
	return y.Tasks.List[index].IsRuning
}

/*
	get task list
*/
func (y *Schedule) Get_TaskList() []Task_Info {
	//logs.Info("Load Task....")
	return y.Tasks.List
}

/*
	stop task
*/
func (y *Schedule) Stop_AllTask() error {
	logs.Info("stop all task...-->begin")
	y.StopTick <- struct{}{}
	logs.Info("stop all task...-->end")
	return nil
}

/*
	根据时间戳匹配任务
*/
func (y *Schedule) matchTime(calendar string, t time.Time) (string, string) {
	var (
		year   string
		month  string
		day    string
		week   string
		hour   string
		minute string
		second string
	)
	times := strings.Split(calendar, "-")
	if len(times) < 6 {
		logs.Info("canlendar format error!")
		return "", ""
	}
	year = times[0]
	month = times[1]
	day = times[2]
	week = times[3]
	hour = times[4]
	minute = times[5]
	second = times[6]
	var calenStr, timeStr string
	if year != "YYYY" {
		calenStr += fmt.Sprintf("Y%s-", year)
		timeStr += fmt.Sprintf("Y%d-", t.Year())
	}
	if month != "MM" {
		calenStr += fmt.Sprintf("M%s-", month)
		timeStr += fmt.Sprintf("M%02d-", t.Month())
	}
	if day != "DD" {
		calenStr += fmt.Sprintf("D%s-", day)
		timeStr += fmt.Sprintf("D%02d-", t.Day())
	}
	if week != "W" {
		calenStr += fmt.Sprintf("W%s-", week)
		switch t.Weekday().String() {
		case "Sunday":
			timeStr += fmt.Sprintf("W%d-", 7)
		case "Monday":
			timeStr += fmt.Sprintf("W%d-", 1)
		case "Tuesday":
			timeStr += fmt.Sprintf("W%d-", 2)
		case "Wednesday":
			timeStr += fmt.Sprintf("W%d-", 3)
		case "Thursday":
			timeStr += fmt.Sprintf("W%d-", 4)
		case "Friday":
			timeStr += fmt.Sprintf("W%d-", 5)
		case "Saturday":
			timeStr += fmt.Sprintf("W%d-", 6)
		}
	}
	if hour != "HH" {
		calenStr += fmt.Sprintf("H%s-", hour)
		timeStr += fmt.Sprintf("H%02d-", t.Hour())
	}
	if minute != "MI" {
		calenStr += fmt.Sprintf("m%s-", minute)
		timeStr += fmt.Sprintf("m%02d-", t.Minute())
	}
	if second != "SE" {
		calenStr += fmt.Sprintf("S%s", second)
		timeStr += fmt.Sprintf("S%02d", t.Second())
	}
	return calenStr, timeStr
}

/*
	实现方案一
	1、利用TICKER没秒产生一个信号，并把期放入CHANNEL中
	2、检测任务的执行时间是否与TICKER产生的时间相符，如果相符执行
*/
func (y *Schedule) Run() error {
	tick := time.NewTicker(CHECK_INTERVAL * time.Second)
	go func() {
		for true {
			select {
			case m := <-y.TickChs:
				for _, v := range y.Get_TaskList() {
					cStr, tStr := y.matchTime(v.Calendar, m)
					if cStr != tStr {
						continue
					}
					logs.Info("匹配到待执行的任务,执行:", cStr, tStr, v, v.IsRuning)
					if v.IsParellel {
						logs.Info("任务处于并行模式,直接执行....")
						go v.RJob.Run(v.Task_Id, nil)
					} else {
						logs.Info("任务处于串行模式,任务结束后再执行....")
						if y.Get_TaskState(v.Task_Id) {
							logs.Info("任务正在运行,返回....")
							continue
						}
						y.Set_TaskState(v.Task_Id, true)
						go v.RJob.Run(v.Task_Id, func(taskId int) {
							y.Set_TaskState(taskId, false)
							logs.Info(taskId, "回调执行...")
						})
					}
				}
			case v := <-y.add:
				y.Tasks.Lock.Lock()
				y.Tasks.List = append(y.Tasks.List, *v)
				y.Tasks.Lock.Unlock()
			}
		}
	}()
	go func(t *time.Ticker) {
		for true {
			select {
			case m := <-t.C:
				y.TickChs <- m
			}
		}
	}(tick)
	logs.Info("Blocking the Run...")
	<-y.StopTick
	logs.Info("Recv a stop siganal ...")
	return nil
}

/*
	实现方案二
	1、利用TICKER没秒产生一个信号，并把期放入CHANNEL中
	2、检测任务的执行时间是否与TICKER产生的时间相符，如果相符执行
*/
