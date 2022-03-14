package router

import (
	"net/http"
	"master/controller"
	"github.com/gin-gonic/gin"
)

func SetUP() *gin.Engine {
	r := gin.Default()

	r.GET("/ping", func(c *gin.Context) {
		c.String(http.StatusOK,"pong")
	})

	v1 := r.Group("/job")
	{
		v1.POST("/save", controller.JobSaveHandler) // 保存任务
		v1.GET("/delete", controller.JobDelHandler) // 删除任务
		v1.GET("/list", controller.JobListHandler) // 获取参数列表
		v1.GET("/kill", controller.JobKillHandler) // 杀死任务
		v1.GET("/log",controller.JobLogListHandler) // 获取任务执行日志
	}

	r.GET("/worker/list", controller.WorkerListHandler) // 获取可用节点

	return r
}