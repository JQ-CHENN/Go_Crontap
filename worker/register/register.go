package register

import (
	"context"
	"errors"
	"log"
	"net"
	"parma"
	"time"

	"github.com/spf13/viper"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// 注册worker节点到 /cron/workers/ip
type Register struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease

	localIP string // 本机ip
}

var R_register *Register

func InitRegister() (err error) {
	config := clientv3.Config{
		Endpoints: viper.GetStringSlice("endpoints"),
		DialTimeout: viper.GetDuration("dialTimeout") * time.Millisecond,
	}

	client, err := clientv3.New(config)
	if err != nil {
		log.Fatal(err)
		return
	}

	localIp, err := getLocalIp()
	if err != nil {
		return
	}

	kv := clientv3.NewKV(client)
	lease := clientv3.NewLease(client)

	R_register = &Register{
		client: client,
		kv: kv,
		lease: lease,

		localIP: localIp,
	}

	// 服务注册
	go R_register.keepAlive()

	return
}

// 获取网卡ip
func getLocalIp() (ipv4 string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}

	for _, addr := range addrs {
		// 判断地址是否是Ip地址
		// 取第一个非Io网卡Ip，跳过虚拟Ip
		// 跳过Ipv6
		ipNet, isIpNet := addr.(*net.IPNet)
		if isIpNet && !ipNet.IP.IsLoopback() && ipNet.IP.To4() != nil {
			ipv4 = ipNet.IP.String()
			return
		}
	}
	err = errors.New(parma.ERR_NO_LOCAL_IP_FOUND)
	return 
}

// 注册到 /cron/workers/ip中, 并自动续租
func (register *Register) keepAlive() {
	
	
	var (
		regKey string
		leaseResp *clientv3.LeaseGrantResponse
		keepAliveChan <-chan *clientv3.LeaseKeepAliveResponse
		//putResp *clientv3.PutResponse
		keepAliveResp *clientv3.LeaseKeepAliveResponse
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		err error
	)

	regKey = parma.JOBS_WORKERS_DIR + register.localIP
	
	for {
		cancelFunc = nil
		if leaseResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}
		if keepAliveChan, err = register.lease.KeepAlive(context.TODO(), leaseResp.ID); err != nil{
			goto RETRY
		}
		
		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		if _, err = register.kv.Put(cancelCtx, regKey, "", clientv3.WithLease(leaseResp.ID)); err != nil {
			goto RETRY
		}
		// 处理续租应答
		for keepAliveResp = range keepAliveChan {
			if keepAliveResp == nil {
				goto RETRY
			}
		}

		RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}