package servicehub

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	loadbalancer "github.com/atopos31/go-velisearch/index_service/load_balancer"
	"github.com/atopos31/go-velisearch/util"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	etcdv3 "go.etcd.io/etcd/client/v3"
)

var (
	etcdServiceHub *EtcdServiceHub // 单例 不对外暴露
	hubOnce        sync.Once
)

type EtcdServiceHub struct {
	client             *etcdv3.Client            // etcd客户端，用于与etcd进行操作
	heartbeatFrequency int64                     // 服务续约的心跳频率，单位：秒
	watched            sync.Map                  // 存储已经监视的服务，以避免重复监视
	loadBalancer       loadbalancer.LoadBalancer // 负载均衡策略的接口，支持多种负载均衡实现
}

// GetServiceHub 获取一个单例的ServiceHub实例
func GetServiceHub(etcdServers []string, heartbeatFrequency int64) *EtcdServiceHub {
	if etcdServiceHub == nil {
		hubOnce.Do(func() {
			client, err := etcdv3.New(etcdv3.Config{
				Endpoints:   etcdServers,
				DialTimeout: 3 * time.Second,
			})

			if err != nil {
				util.Log.Println("Error creating etcd client:", err)
			}

			etcdServiceHub = &EtcdServiceHub{
				client:             client,
				heartbeatFrequency: heartbeatFrequency,
				watched:            sync.Map{},
				loadBalancer:       &loadbalancer.RoundRobin{},
			}
		})
	}

	return etcdServiceHub
}

func (hub *EtcdServiceHub) RegisterService(service string, endpoint string, leaseID etcdv3.LeaseID) (etcdv3.LeaseID, error) {
	if leaseID <= 0 {
		// 创建租约
		leaseResp, err := hub.client.Grant(context.Background(), hub.heartbeatFrequency)
		if err != nil {
			util.Log.Printf("Error creating lease: %v", err)
			return 0, err
		}
		key := ServiceRootPath + "/" + service + "/" + endpoint
		_, err = hub.client.Put(context.Background(), key, endpoint, etcdv3.WithLease(leaseResp.ID))
		if err != nil {
			util.Log.Printf("Error registering service: %v", err)
			return 0, err
		}
		return leaseResp.ID, nil
	} else {
		// 更新租约
		_, err := hub.client.KeepAliveOnce(context.Background(), leaseID)
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			// 租约不存在，重新注册服务
			return hub.RegisterService(service, endpoint, 0)
		} else if err != nil {
			util.Log.Printf("Error keeping lease alive: %v", err)
		}

		return leaseID, nil
	}
}

// UnregisterService 注销服务
func (hub *EtcdServiceHub) UnregisterService(service string, endpoint string) error {
	key := ServiceRootPath + "/" + service + "/" + endpoint
	_, err := hub.client.Delete(context.Background(), key)
	if err != nil {
		util.Log.Printf("Error unregistering service: %v", err)
		return err
	}

	util.Log.Printf("Service %s unregistered successfully", endpoint)
	return nil
}

// GetServiceEndpoints 获取服务对应的所有端点
func (hub *EtcdServiceHub) GetServiceEndpoints(service string) []string {
	prefix := ServiceRootPath + "/" + service + "/"
	getResp, err := hub.client.Get(context.Background(), prefix, etcdv3.WithPrefix())
	if err != nil {
		util.Log.Printf("Error getting service endpoints: %v", err)
		return nil
	}
	endpoints := make([]string, len(getResp.Kvs))
	for _, kv := range getResp.Kvs {
		path := strings.Split(string(kv.Key), "/")
		endpoints = append(endpoints, path[len(path)-1])
	}

	util.Log.Printf("News Endpoints for service %s: %v", service, endpoints)
	return endpoints
}

// GetServiceEndpoint 获取负载均衡的服务端点
func (hub *EtcdServiceHub) GetServiceEndpoint(service string) string {
	endpoints := hub.GetServiceEndpoints(service)
	return hub.loadBalancer.Take(endpoints)
}

// Close 关闭ServiceHub
func (hub *EtcdServiceHub) Close() {
	// 尝试关闭etcd客户端连接
	err := hub.client.Close()
	if err != nil {
		// 如果关闭连接失败，记录错误日志
		util.Log.Printf("关闭etcd客户端连接失败: %v", err)
	}
}
