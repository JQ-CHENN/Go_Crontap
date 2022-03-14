package logic

import (
	"errors"
	"fmt"
	"time"

	"parma"
)

// 任务调度
type Scheduler struct {
	jobEventChan chan *parma.JobEvent // 任务事件队列
	jobPlanTable map[string]*parma.JobSchedulePlan // 任务调度表
	jobExecutingTable map[string]*parma.JobExecInfo // 任务执行表
	jobResChan chan *parma.JobExecRes // 任务执行结果队列
}

var L_scheduler *Scheduler

func InitScheduler() (err error) {
	L_scheduler = &Scheduler{
		jobEventChan: make(chan *parma.JobEvent, 1024),
		jobPlanTable: make(map[string]*parma.JobSchedulePlan),
		jobExecutingTable: make(map[string]*parma.JobExecInfo),
		jobResChan: make(chan *parma.JobExecRes, 1024),
	}
	// 启动调度协程
	go L_scheduler.scheduleLoop()
	return
}

// 同步任务变化事件
func (scheduler *Scheduler) PutJobEvent(event *parma.JobEvent) {
	scheduler.jobEventChan <- event
}

// 回传任务执行结果
func (scheduler *Scheduler) PushJobRes(jobRes *parma.JobExecRes) {
	scheduler.jobResChan <- jobRes
}

// 尝试执行任务
func (schedule *Scheduler) TryStartJob(plan *parma.JobSchedulePlan) {
	// 调度后执行
	// 执行的任务可能在下次调度时还没执行完毕,所以要避免任务的并发执行
	// 如果状态正在执行，跳过
	if _, executing := schedule.jobExecutingTable[plan.Job.Name]; executing {
		//fmt.Println("执行中，已跳过:", plan.Job.Name)
		return
	}

	// 构建执行状态信息
	jobExecInfo := parma.BuildJobExecInfo(plan)
	// 保存执行状态
	schedule.jobExecutingTable[jobExecInfo.Job.Name] = jobExecInfo

	// 执行任务
	// 执行任务的shell命令
	fmt.Println("执行:", jobExecInfo.Job.Name, jobExecInfo.PlanTime, jobExecInfo.RealTime)
	L_executor.ExecJob(jobExecInfo)
}

// 扫描任务状态
func (schedule *Scheduler) ScanJobPlan() (after time.Duration) {
	var (
		now time.Time
		nearTime *time.Time
	)

	// 任务表为空
	if len(schedule.jobPlanTable) == 0 {
		after = 1 * time.Second
		return
	}

	// 遍历所有任务
	for _, plan := range schedule.jobPlanTable {
		now = time.Now()
		if plan.NextTime.Before(now) || plan.NextTime.Equal(now) {
			// 尝试执行过期任务
			schedule.TryStartJob(plan)
			plan.NextTime = plan.Expr.Next(now)
		} 
		// 统计最近过期时间
		if nearTime == nil || plan.NextTime.Before(*nearTime) {
			nearTime = &plan.NextTime
		}
	}

	// 下次调度任务间隔
	after = (*nearTime).Sub(now)

	return
}

// 处理任务变化
func (scheduler *Scheduler) handleEvent (event *parma.JobEvent) {
	switch event.EventType {
	case parma.EVENT_SAVE:
		plan, err := parma.BuildJobPlan(event.Job)
		if err != nil {
			return
		}
		scheduler.jobPlanTable[event.Job.Name] = plan
	case parma.EVENT_DEL:
		delete(scheduler.jobPlanTable, event.Job.Name)
	case parma.EVENT_KILL:
		// 取消Command执行
		if execInfo, executing := scheduler.jobExecutingTable[event.Job.Name]; executing {
			execInfo.CancelFunc() // 触发Command杀死指定程序
		}
	}
}

// 处理任务执行结果
func (scheduler *Scheduler) handleRes(jobRes *parma.JobExecRes) {
	delete(scheduler.jobExecutingTable, jobRes.ExecInfo.Job.Name)
	
	if jobRes.Err != errors.New(parma.ERR_LOCK_NEED) {
		// 生成执行日志
		jobLog := &parma.JobLog{
		JobName: jobRes.ExecInfo.Job.Name,
		Command: jobRes.ExecInfo.Job.Command,
		Output: string(jobRes.Output),

		PlanTime: jobRes.ExecInfo.PlanTime,
		ScheduleTime: jobRes.ExecInfo.RealTime,

		StartTime: jobRes.StartTime,
		EndTime: jobRes.EndTime,
		}

		if jobRes.Err != nil {
			jobLog.Err = jobRes.Err.Error()
		}

		// 将日志存储到MongoDB
		L_Logger.AppendLog(jobLog)
	}

	fmt.Println("任务执行完成:", jobRes.ExecInfo.Job.Name, string(jobRes.Output), jobRes.Err)
}

// 调度协程
func (scheduler *Scheduler) scheduleLoop() {
	after := scheduler.ScanJobPlan()
	timer := time.NewTimer(after)
	for { // 监听任务变化事件
		select {
		case jobEvent := <- scheduler.jobEventChan:
			// 维护任务列表
			scheduler.handleEvent(jobEvent)
		case <-timer.C: // 最近的任务过期了
		case jobRes := <- scheduler.jobResChan: // 监听任务执行结果
			scheduler.handleRes(jobRes)
		}

		after = scheduler.ScanJobPlan()
		// 重置定时器
		timer.Reset(after)
	}
}