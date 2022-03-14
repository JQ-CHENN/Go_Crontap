package mongodb

import (
	"context"
	"parma"
	"time"

	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type JobLogManager struct {
	client *mongo.Client
	logCollection *mongo.Collection
}

var Logger *JobLogManager

func InitLogger() (err error) {
	MongoURI := viper.GetString("MongoDBURI")
	MongoDB_TimeOut := viper.GetInt("MongoDB_TimeOut")
	client, err := mongo.Connect(
		context.TODO(),
		options.Client().ApplyURI(MongoURI).
		SetConnectTimeout(time.Duration(MongoDB_TimeOut) * time.Millisecond),
	)

	Logger = &JobLogManager{
		client: client,
		logCollection: client.Database("cron").Collection("log"),
	}

	return
}

// 查看任务日志列表
func (logger *JobLogManager) ListLog(name string, start int64, size int64) (list []*parma.JobLog, err error) {
	list = make([]*parma.JobLog, 0)
	
	filter := bson.D{{Key:"jobName", Value:name}}
	cursor, err := logger.logCollection.Find(
		context.TODO(), 
		filter, 
		options.Find().SetSort(bson.D{{Key:"_id", Value: -1}}),
		options.Find().SetSkip(start),
		options.Find().SetLimit(size),
	) // 倒序
	if err != nil {
		return
	}
	defer cursor.Close(context.TODO())

	for cursor.Next(context.TODO()) {
		jobLog := &parma.JobLog{}
		if err := cursor.Decode(jobLog); err != nil {
			continue
		}
		list = append(list, jobLog)
	}

	return
}