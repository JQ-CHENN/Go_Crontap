package logic

import "master/dao/etcd"

func WorkerList() (list []string, err error) {
	list, err = etcd.E_WorkerMgr.WorkerList()
	return
}