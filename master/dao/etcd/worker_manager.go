package etcd

import (
	"context"
	"log"
	"parma"
	"strings"
	"time"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type WokerMgr struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var E_WorkerMgr *WokerMgr 

func InitWorkerMgr() (err error) {
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

	E_WorkerMgr = &WokerMgr{
		client: client,
		kv: kv,
		lease: lease,

	}

	return
}

// 获取在线worker列表
func (workerMgr *WokerMgr) WorkerList() (list []string ,err error) {
	list = make([]string, 0)
	getResp, err := workerMgr.kv.Get(context.TODO(), parma.JOBS_WORKERS_DIR, clientv3.WithPrefix())
	if err != nil {
		return
	}

	for _, kv := range getResp.Kvs {
		ip := strings.TrimPrefix(string(kv.Key), parma.JOBS_WORKERS_DIR)
		list = append(list, ip)
	}

	return
}