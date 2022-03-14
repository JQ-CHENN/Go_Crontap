package logic

import (
	"context"
	"log"
	"time"

	"parma"

	"github.com/spf13/viper"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var EtcdManager *JobManager

// 任务管理器
type JobManager struct {
	Client *clientv3.Client
	Kv clientv3.KV
	Lease clientv3.Lease
	watcher clientv3.Watcher
}


func InitEtcd() (err error) {
	config := clientv3.Config{
		Endpoints: viper.GetStringSlice("endpoints"),
		DialTimeout: viper.GetDuration("dialTimeout") * time.Millisecond,
	}

	client, err := clientv3.New(config)
	if err != nil {
		log.Fatal(err)
		return
	}

	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)
	watcher := clientv3.NewWatcher(client)

	EtcdManager = &JobManager{client, kv, lease, watcher}

	// 启动任务监听
	EtcdManager.WatchJobs()
	// 启动任务killer
	EtcdManager.WatchKiller()

	return
}

// 创建执行任务锁
func (jobMgr *JobManager) CreateJobLock(jobName string) *parma.JobLock {
	return parma.InitJobLock(jobName, jobMgr.Kv, jobMgr.Lease)
}

// 监听强杀任务通知 /cron/kill
func (jobMgr *JobManager) WatchKiller() {
	go func() {
		// 监听 "/cron/kill" 目录下的后续变化
		watchChan := jobMgr.watcher.Watch(context.TODO(), parma.JOBS_KILL_DIR, clientv3.WithPrefix())
		
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				var jobEvent *parma.JobEvent
				switch event.Type {
				case mvccpb.PUT: //杀死任务时间
					jobName := parma.ExtractKillName(string(event.Kv.Key))
					jobEvent = parma.BuildJobEvent(parma.EVENT_KILL, &parma.Job{Name: jobName})
					// 同步kill事件给schedule
					L_scheduler.PutJobEvent(jobEvent)
				case mvccpb.DELETE: //killer标记的任务到期
				}
			}
		}
	}()
}

// 监听任务变化
func (jobMgr *JobManager) WatchJobs() (err error){
	// get "/cron/jobs"下的所有任务，获取当前集群的revision
	getResp, err := jobMgr.Kv.Get(context.TODO(), parma.JOBS_SAVE_DIR, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	// 遍历当前有哪些任务
	for _, kvpair := range getResp.Kvs {
		job, _ := parma.UnpackJob(kvpair.Value)
		jobEvent := parma.BuildJobEvent(parma.EVENT_SAVE, job)
		// 把job同步给scheduler(调度协程)
		L_scheduler.PutJobEvent(jobEvent)
	}
	// 从该revision监听后续变化事件
	go func() {
		watchStartRevison := getResp.Header.Revision + 1
		// 监听 "/cron/jobs" 目录下的后续变化
		watchChan := jobMgr.watcher.Watch(context.TODO(), parma.JOBS_SAVE_DIR, 
			clientv3.WithRev(watchStartRevison), clientv3.WithPrefix())
		
		for watchResp := range watchChan {
			for _, event := range watchResp.Events {
				var jobEvent *parma.JobEvent
				switch event.Type {
				case mvccpb.PUT: //任务保存事件
					// 反序列化job,同步更新事件给scheduler
					job, err := parma.UnpackJob(event.Kv.Value); 
					if err != nil {
						continue
					}
					// 构造一个更新Event
					jobEvent = parma.BuildJobEvent(parma.EVENT_SAVE, job)
					
				case mvccpb.DELETE: //任务删除事件
					// delete /cron/jobs/jobname
					jobName := parma.ExtractJobName(string(event.Kv.Key))
					// 构造一个删除Event
					jobEvent = parma.BuildJobEvent(parma.EVENT_DEL, &parma.Job{Name: jobName})
				    
				}
				// 同步删除或更新事件给schedule
				L_scheduler.PutJobEvent(jobEvent)
			}
		}
	}()
	return
}
