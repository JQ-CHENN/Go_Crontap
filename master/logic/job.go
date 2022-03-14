package logic

import (
	"context"
	"encoding/json"

	//"errors"
	"master/dao/etcd"
	"master/dao/mongodb"
	"parma"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func JobSave(job *parma.Job) (data interface{}, err error) {
	//leaseID := etcd.EtcdManager.Lease.Grant()
	jobKey := parma.JOBS_SAVE_DIR + job.Name
	jobVal, _ := json.Marshal(job)
	putResp, err := etcd.E_JobManager.Kv.Put(context.TODO(), jobKey, string(jobVal), clientv3.WithPrevKV())
	
	oldJob := &parma.Job{}

	if putResp.PrevKv != nil { // 更新旧的值
		json.Unmarshal(putResp.PrevKv.Value, oldJob)
		data = oldJob
	}
	
	return
}

func JobDel(name string) (data interface{}, err error){
	jobKey := parma.JOBS_SAVE_DIR + name
	
	delResp, err := etcd.E_JobManager.Kv.Delete(context.TODO(), jobKey, clientv3.WithPrevKV())
	if err != nil {
		return
	} 

	oldJob := new(parma.Job)
	if len(delResp.PrevKvs) != 0 {
		json.Unmarshal(delResp.PrevKvs[0].Value, oldJob)
		data = oldJob
	} //else {
	// 	err = errors.New("delete no key")
	// }
	
	return
}

func JobList() (jobs []*parma.Job, err error) {
	jobs = make([]*parma.Job, 0)
	jobKey := parma.JOBS_SAVE_DIR
	getResp, err := etcd.E_JobManager.Kv.Get(context.TODO(), 
					jobKey, clientv3.WithPrefix())

	if err != nil {
		return
	}

	for _, kvPair := range getResp.Kvs {
		job := &parma.Job{}
		if err = json.Unmarshal(kvPair.Value, job); err != nil {
			err = nil
			continue
		}

		jobs = append(jobs, job)
	}
	return
}

func JobKill(jobName string) (err error) {

	jobKey := parma.JOBS_KILL_DIR + jobName
	
	leaseResp, err := etcd.E_JobManager.Lease.Grant(context.TODO(), 1)
	if err != nil {
		return
	}
	
	leaseID := leaseResp.ID
	_, err = etcd.E_JobManager.Kv.Put(context.TODO(), jobKey, "", clientv3.WithLease(leaseID))
	
	return
}

func JobLogList(listParma *parma.JobLogListParma) (loglist []*parma.JobLog, err error) {
	loglist, err = mongodb.Logger.ListLog(listParma.Name, listParma.Start, listParma.Size)
	return
}