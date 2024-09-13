package servicehub

import etcdv3 "go.etcd.io/etcd/client/v3"

type ServiceHub interface {
	RegisterService(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) // 注册服务
	UnregisterService(service string, endpoint string) error                                         // 注销服务
	GetServiceEndpoints(service string) []string                                                     // 服务发现
	GetServiceEndpoint(service string) string                                                        // 选择服务的一个endpoint
	Close()                                                                                          // 关闭etcd客户端连接
}
