package etcd

import (
	"log"
	"time"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var E_JobManager *JobManager

// 任务管理器
type JobManager struct {
	Client *clientv3.Client
	Kv clientv3.KV
	Lease clientv3.Lease
}


func InitJobMgr() (err error) {
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

	E_JobManager = &JobManager{client, kv, lease}

	return
}