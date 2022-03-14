module worker

go 1.16

require (
	github.com/spf13/viper v1.10.1
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	go.mongodb.org/mongo-driver v1.8.4
	parma v0.0.0-00010101000000-000000000000
)

replace master => ../master

replace parma => ../parma
