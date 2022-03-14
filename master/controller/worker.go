package controller

import (
	"log"
	"master/logic"
	"parma"

	"github.com/gin-gonic/gin"
)

func WorkerListHandler(c *gin.Context) {
	data, err := logic.WorkerList()
	if err != nil {
		log.Fatal("get worker list failed:", err)
		Response(c, -1, parma.GET_WORKERS_FAILED, nil)
	}

	Response(c, 0, parma.GET_WORKERS_SUCCESS, data)
}