package servicehub

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/atopos31/go-velisearch/util"
	etcdv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/time/rate"
)

var (
	hubProxy  *HubProxy
	proxyOnce sync.Once
)

type HubProxy struct {
	*EtcdServiceHub               // 真实的ServiceHub实例
	endpointCache   sync.Map      // 缓存服务端点
	limiter         *rate.Limiter // 限流器
}

func GetServiceHubProxy(etcdServers []string, heartbeatFrequency int64, qps int) *HubProxy {
	if hubProxy == nil {
		proxyOnce.Do(func() {
			// 初始化HubProxy实例
			hubProxy = &HubProxy{
				EtcdServiceHub: GetServiceHub(etcdServers, heartbeatFrequency),
				endpointCache:  sync.Map{},
				// 配置限流器：每秒产生qps个令牌
				limiter: rate.NewLimiter(rate.Every(time.Duration(1e9/qps)*time.Nanosecond), qps),
			}

		})
	}
	return hubProxy
}

func (p *HubProxy) GetServiceEndpoints(service string) []string {
	if !p.limiter.Allow() {
		return nil
	}

	p.watchEndpointsOfService(service)

	cachedEndpoints, ok := p.endpointCache.Load(service)
	if !ok {
		endpoints := p.EtcdServiceHub.GetServiceEndpoints(service)
		if len(endpoints) > 0 {
			// 如果查询到端点，将其存入缓存
			p.endpointCache.Store(service, endpoints)
		}
		return endpoints
	}
	return cachedEndpoints.([]string)
}

func (p *HubProxy) watchEndpointsOfService(service string) {
	_, ok := p.watched.LoadOrStore(service, true)
	if ok {
		return
	}

	prefix := strings.TrimRight(ServiceRootPath, "/") + "/" + service + "/"
	watchChan := p.client.Watch(context.Background(), prefix, etcdv3.WithPrefix())
	util.Log.Printf("开始监视服务端点: %s", prefix)

	go func() {
		for response := range watchChan {
			for _, event := range response.Events {
				util.Log.Printf("etcd事件类型: %s", event.Type)

				path := strings.Split(string(event.Kv.Key), "/")
				if len(path) > 2 {
					service := path[len(path)-2]
					endpoints := p.EtcdServiceHub.GetServiceEndpoints(service)
					if len(endpoints) > 0 {
						p.endpointCache.Store(service, endpoints)
					} else {
						p.endpointCache.Delete(service)
					}
				}
			}
		}
	}()
}
