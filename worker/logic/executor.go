package logic

import (
	"math/rand"
	"os/exec"
	"time"

	"parma"
)

type Executor struct {

}

var L_executor *Executor

func InitExecutor() (err error) {
	L_executor = &Executor{}

	return
}

func (executor *Executor) ExecJob(info *parma.JobExecInfo) {
	go func() { 
		execRes := &parma.JobExecRes{
			ExecInfo: info,
			Output: make([]byte, 0),
		}

		// 获取分布式锁
		lock := EtcdManager.CreateJobLock(info.Job.Name)
		execRes.StartTime = time.Now()

		// 上锁
		// 随机睡眠(0~1s)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond) 
		err := lock.TryLock()

		defer lock.UnLock()

		if err != nil {
			execRes.Err = err
			execRes.EndTime = time.Now()
		} else {
			execRes.StartTime = time.Now()
			// 执行shell命令
			cmd := exec.CommandContext(info.CancelCtx, "/bin/bash", "-c", info.Job.Command)
			// 执行并捕获输出
			execRes.Output, execRes.Err = cmd.CombinedOutput()

			// 任务执行完成, 构造执行结果返回给scheduler, 
			// scheduler从jobExecutingTable中删除对应信息
			execRes.EndTime = time.Now()
		}

		L_scheduler.PushJobRes(execRes)
	}()
}
