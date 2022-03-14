package logic

import (
	"context"
	"parma"
	"time"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Logger struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *parma.JobLog
	autoCommitChan chan *parma.JobLogBatch
}

var L_Logger *Logger

func InitLogger() (err error) {
	// 建立MongoDB连接
	MongoDBURI := viper.GetString("MongoDBURI")
	MongoDB_TimeOut := viper.GetInt("MongoDB_TimeOut")
	client, err := mongo.Connect(context.TODO(), 
		options.Client().ApplyURI(MongoDBURI).
		SetConnectTimeout(time.Duration(MongoDB_TimeOut) * time.Millisecond))
	
	// 选择db和collection
	L_Logger = &Logger{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
		logChan: make(chan *parma.JobLog, 1024),
		autoCommitChan: make(chan *parma.JobLogBatch, 1024),
	}
	
	// 启动一个MongoDB处理协程
	go func() {
		var (
			logBatch *parma.JobLogBatch
			LogBatchSize = viper.GetInt("LogBatchSize")
			commitTimer *time.Timer
		)

		for {
			select {
			case log := <- L_Logger.logChan:
				if logBatch == nil {
					logBatch = &parma.JobLogBatch{}
					// 使批次超时自动提交
					commitTimer = time.AfterFunc(
						viper.GetDuration("LogCommitTimeOut") * time.Millisecond,
						func(batch *parma.JobLogBatch) func() {
							return func() {
								// 发出超时通知
								L_Logger.autoCommitChan <- logBatch
							}
						}(logBatch),
					)
				}
				// 将日志追加到批次中
				logBatch.Logs = append(logBatch.Logs, log)
				// 如果批次满了,写入数据
				if len(logBatch.Logs) >= LogBatchSize {
					L_Logger.logCollection.InsertMany(context.TODO(), logBatch.Logs)
					logBatch = nil
					commitTimer.Stop()
				}
				
			case timeOutBatch := <- L_Logger.autoCommitChan:
				// 判断过期批次是否仍是当前批次
				if timeOutBatch == logBatch {
					// 把过期日志写入MongoDB中
					L_Logger.logCollection.InsertMany(context.TODO(), timeOutBatch.Logs)
					logBatch = nil
				}
			}
		}
	}()

	return
}

// 发送日志
func (logger *Logger) AppendLog(jobLog *parma.JobLog) {
	select {
	case logger.logChan <- jobLog:
	default://队列满日志丢弃
	}
}	