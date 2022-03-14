package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"master/dao/etcd"
	"master/dao/mongodb"
	"master/router"
	"master/settings"

	"github.com/spf13/viper"
)


func main() {
	// 初始化线程
	runtime.GOMAXPROCS(runtime.NumCPU())
	// 读取配置文件
	if err := settings.Init(); err != nil {
		log.Fatal("初始化配置失败...")
		return
	}

	// 初始化worker管理器(服务发现)
	if err := etcd.InitWorkerMgr(); err != nil {
		log.Fatal("etcd workermgr init falied...")
		return
	}

	// 初始化日志管理器
	if err := mongodb.InitLogger(); err != nil {
		log.Fatal("mongodb logger init falied...")
	}

	// 初始化任务管理器
	if err := etcd.InitJobMgr(); err != nil {
		log.Fatal("etcd jobmgr init falied...")
		return
	}
	
	// 初始化路由
	r := router.SetUP()
	// 启动服务
	srv := &http.Server{
		Addr: fmt.Sprintf(":%d", viper.GetInt("port")),
		Handler: r,
		ReadTimeout: time.Duration(viper.GetInt("read_timeout")) * time.Millisecond,
		WriteTimeout: time.Duration(viper.GetInt("write_timeout")) * time.Millisecond,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatal("listen failed...")
		}
	}()

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Fatal("Shutdown Server ...")

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}
