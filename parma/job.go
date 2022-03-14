package parma

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/gorhill/cronexpr"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Job struct {
	Name string `json:"name" binding:"required"`
	Command string `json:"command" binding:"required"`
	CronExpr string `json:"cron_expr" binding:"required"`
}

// 任务事件
type JobEvent struct {
	EventType int // 更新  删除
	Job *Job
}

// 任务调度计划
type JobSchedulePlan struct {
	Job *Job
	Expr *cronexpr.Expression
	NextTime time.Time
}

// 任务执行信息
type JobExecInfo struct {
	Job *Job // 正在执行的任务
	PlanTime time.Time // 调度时间
	RealTime time.Time // 实际调度时间
	CancelCtx context.Context // 取消上下文
	CancelFunc context.CancelFunc // 用于取消任务执行
}

// 任务执行结果
type JobExecRes struct {
	ExecInfo *JobExecInfo
	StartTime time.Time // 执行开始时间
	EndTime time.Time // 执行结束时间
	Output []byte
	Err error
}

// 任务执行日志
type JobLog struct {
	JobName string `bson:"jobName"`
	Command string `bson:"command"`
	Err string `bson:"error"`
	Output string `bson:"output"` // 脚本输出
	
	PlanTime time.Time `bson:"plan_time"` // 任务计划执行时间
	ScheduleTime time.Time `bson:"schedule_time"` // 任务调度时间

	StartTime time.Time `bson:"start_time"` // 任务开始执行时间
	EndTime time.Time `bson:"end_time"` // 任务执行结束时间
}

// 日志批次
type JobLogBatch struct {
	Logs []interface{}

}

// 任务日志列表，用于返回给给前端
type JobLogListParma struct {
	Name string `json:"name" form:"name"`
	Start int64 `json:"start" form:"start"`
	Size int64 `json:"size" form:"size"`
}



// 分布式锁(TXN事务)
type JobLock struct {
	JobName string

	Kv clientv3.KV
	Lease clientv3.Lease
	LeaseID clientv3.LeaseID
	CancelFunc context.CancelFunc
	IsLock bool
}

func InitJobLock(jobName string, kv clientv3.KV, lease clientv3.Lease) (lock *JobLock) {
	lock = &JobLock{
		JobName: jobName,
		Kv: kv,
		Lease: lease,
	}
	return
}

// 抢锁
func (jobLock *JobLock) TryLock() (err error) {
	var (
		txn clientv3.Txn
		lockKey string
		txnResp *clientv3.TxnResponse
	)
	// 创建一个租约
	leaseResp, err := jobLock.Lease.Grant(context.TODO(), 5)
	if err != nil {
		return
	}
	// 用于取消自动续租
	cancelCtx, cancelFunc := context.WithCancel(context.TODO())
	// 自动续租
	leaseID := leaseResp.ID
	KeepAliveChan, err := jobLock.Lease.KeepAlive(cancelCtx, leaseID)
	if err != nil {
		goto ERR
	}

	go func() { // 处理续租应答
		for keepResp := range KeepAliveChan {
			if keepResp == nil {
				goto END
			}
		}
		END:
	}()

	// 创建事务
	txn = jobLock.Kv.Txn(context.TODO())
	lockKey = JOBS_LOCK_DIR + jobLock.JobName
	// 事务抢锁
	txn.If(clientv3.Compare(clientv3.CreateRevision(lockKey), "=", 0)).
		Then(clientv3.OpPut(lockKey, "", clientv3.WithLease(leaseID))).
		Else(clientv3.OpGet(lockKey))
	
	// 提交事务
	if txnResp, err = txn.Commit(); err != nil {
		goto ERR
	}
	// 成功返回，失败释放租约
	if !txnResp.Succeeded {
		err = errors.New(ERR_LOCK_NEED)
		goto ERR
	}

	// 抢锁成功
	jobLock.LeaseID = leaseID
	jobLock.CancelFunc = cancelFunc
	jobLock.IsLock = true
	return

ERR:
	cancelFunc() // 取消自动续租
	jobLock.Lease.Revoke(context.TODO(), leaseID) // 释放租约
	return
}
// 释放锁
func (jobLock *JobLock) UnLock() {
	if jobLock.IsLock {
		jobLock.CancelFunc()
		jobLock.Lease.Revoke(context.TODO(), jobLock.LeaseID)
	}
}

// 反序列化job
func UnpackJob(value []byte) (ret *Job, err error) {
	job := &Job{}
	if err = json.Unmarshal(value, job); err != nil {
		return
	}

	ret = job
	return
}

// 从etcd的key中获取任务名 /cron/jobs/xxx
func ExtractJobName(key string) string {
	return strings.TrimPrefix(key, JOBS_SAVE_DIR)
}

// 从etcd的key中获取任务名 /cron/kill/xxx
func ExtractKillName(key string) string {
	return strings.TrimPrefix(key, JOBS_KILL_DIR)
}


// 封装任务变化事件
func BuildJobEvent(eventType int, job *Job) *JobEvent {
	return &JobEvent{
		EventType: eventType,
		Job: job,
	}
}

// 构造执行状态信息
func BuildJobExecInfo(plan *JobSchedulePlan) (jobExecInfo *JobExecInfo) {
	jobExecInfo = &JobExecInfo{
		Job: plan.Job,
		PlanTime: plan.NextTime,
		RealTime: time.Now(),
	}
	jobExecInfo.CancelCtx, jobExecInfo.CancelFunc = context.WithCancel(context.TODO())
	return
}

// 构造任务执行计划
func BuildJobPlan(job *Job) (plan *JobSchedulePlan, err error){
	expr, err := cronexpr.Parse(job.CronExpr)
	if err != nil {
		return
	}

	plan = &JobSchedulePlan{
		Job: job,
		Expr: expr,
		NextTime: expr.Next(time.Now()),
	}

	return
}