package main

import (
	"log"
	"runtime"
	"time"

	"worker/logic"
	"worker/register"
	"worker/settings"
)

func main() {
	// 初始化线程
	runtime.GOMAXPROCS(runtime.NumCPU())
	// 读取配置文件
	if err := settings.Init(); err != nil {
		log.Fatal("初始化配置失败...")
		return
	}

	if err := register.InitRegister(); err != nil {
		log.Fatal("register init err")
		return
	}

	// 初始化MongoDB并启动
	if err := logic.InitLogger(); err != nil {
		log.Fatal("logger init err")
		return
	}
	
	//初始化executor并启动
	if err := logic.InitExecutor(); err != nil {
		log.Fatal("executor init err")
		return
	}

	// 初始化调度服务
	if err := logic.InitScheduler(); err != nil {
		log.Fatal("scheduler init err")
		return
	}

	// 初始化etcd连接
	if err := logic.InitEtcd(); err != nil {
		log.Fatal("etcd init falied...")
		return
	}

	for {
		time.Sleep(1 * time.Second)
	}
}