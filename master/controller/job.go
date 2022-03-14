package controller

import (
	"fmt"
	"log"

	"master/logic"
	"parma"

	"github.com/gin-gonic/gin"
)

func JobSaveHandler(c *gin.Context) {
	job := new(parma.Job)
	if err := c.ShouldBindJSON(job); err != nil {
		log.Fatal("job parms invalid:",err)
	}
	data, err := logic.JobSave(job)
	if err != nil {
		log.Fatal(err)
		Response(c, -1, parma.SAVE_FAILED, nil)
		return 
	}

	Response(c, 0, parma.SAVE_SUCCESS, data)
}

func JobDelHandler(c *gin.Context) {
	var (
		jobName string 
		data interface{}
		err error
	)
	jobName = c.Query("name")
	if jobName == "" {
		goto ERR
	}

	data, err = logic.JobDel(jobName)
	if err != nil {
		goto ERR
	}

	Response(c, 0, parma.DEL_SUCCESS, data)
	return

ERR: 
	log.Fatal(err)
	Response(c, -1, parma.DEL_FAILED, nil)
}

func JobListHandler(c *gin.Context) {
	data, err := logic.JobList()

	if err != nil {
		log.Fatal(err)
		Response(c, -1, parma.GET_JOBS_FAILED, nil)
		return
	}

	Response(c, 0, parma.GET_JOBS_SUCCESS, data)
}

func JobKillHandler(c *gin.Context) {
	jobName := c.Query("name")
	fmt.Println(jobName)
	if err := logic.JobKill(jobName); err != nil {
		Response(c, -1, parma.KILL_JOB_FAILED, nil)
		return
	}

	Response(c, 0, parma.KILL_JOB_SUCCESS, nil)
}

func JobLogListHandler(c *gin.Context) {
	joblist := new(parma.JobLogListParma)
	if err := c.ShouldBindQuery(joblist); err != nil {
		log.Fatal("joblist parms invalid:",err)
		return
	}

	data, err := logic.JobLogList(joblist)
	if err != nil {
		log.Fatal(err)
		Response(c, -1, parma.LOG_NOT_OK, nil)
		return 
	}

	Response(c, 0, parma.LOG_OK, data)
}