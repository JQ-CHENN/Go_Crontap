package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type ResponseData struct {
	Code int `json:"code"`
	Msg string `json:"msg"`
	Data interface{} `json:"data"`
}

func Response(c *gin.Context, code int, msg string, data interface{}) {
	c.JSON(http.StatusOK, &ResponseData{
		Code: code,
		Msg: msg,
		Data: data,
	})
}