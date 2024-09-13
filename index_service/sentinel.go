package indexservice

import (
	context "context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	servicehub "github.com/atopos31/go-velisearch/index_service/service_hub"
	types "github.com/atopos31/go-velisearch/types"
	"github.com/atopos31/go-velisearch/util"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

type Sentinel struct {
	hub      servicehub.ServiceHub // 服务发现中心
	connPool sync.Map              // 缓存与Worker的连接
}

func NewSentinel(etcdServers []string) *Sentinel {
	return &Sentinel{
		hub:      servicehub.GetServiceHubProxy(etcdServers, 3, 100),
		connPool: sync.Map{},
	}
}

func (s *Sentinel) GetGrpcConn(endpoint string) *grpc.ClientConn {
	v,exist := s.connPool.Load(endpoint)
	if exist {
		conn := v.(*grpc.ClientConn)

		state := conn.GetState()
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			util.Log.Printf("连接到 endpoint %s 的状态为 %s",endpoint,state.String())
			conn.Close()
			s.connPool.Delete(endpoint)
		} else {
			return conn
		}

	}

	cts,cancel := context.WithTimeout(context.Background(), 200*time.Microsecond)
	defer cancel()

	grpcConn , err := grpc.DialContext(cts, endpoint,  grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		util.Log.Printf("连接 %s 失败: %s",endpoint,err)
		return nil
	}

	util.Log.Printf("连接 %s 成功",endpoint)
	s.connPool.Store(endpoint,grpcConn)
	return grpcConn
}

func (s *Sentinel) AddDoc(doc types.Document) (int ,error) {
	endpoint := s.hub.GetServiceEndpoint(IndexService)
	if len(endpoint) == 0 {
		return 0,fmt.Errorf("未找到服务%s 的有效节点",IndexService)
	}

	conn := s.GetGrpcConn(endpoint)
	if conn == nil {
		return 0,fmt.Errorf("无法连接到 %s",endpoint)
	}

	client := NewIndexServiceClient(conn)
	affetcd,err := client.AddDoc(context.Background(),&doc)
	if err != nil {
		return 0,err
	}
	util.Log.Printf("向 %s 添加文档 %s 成功",endpoint,doc.Id)
	return int(affetcd.Count),nil
}

func (s *Sentinel) Delete(docId string) int {
	endpoints := s.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return 0
	}

	var n int32
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _,endpoint := range endpoints {
		go func (endpoint string)  {
			defer wg.Done()
			conn := s.GetGrpcConn(endpoint)
			if conn == nil {
				util.Log.Printf("无法连接到 %s",endpoint)
				return
			}
			client := NewIndexServiceClient(conn)
			affetcd,err := client.DeleteDoc(context.Background(),&DocId{docId})
			if err != nil {
				util.Log.Printf("向 %s 删除文档 %s 失败: %s",endpoint,docId,err)
				return
			}
			if affetcd.Count > 0 {
				atomic.AddInt32(&n,affetcd.Count)
				util.Log.Printf("向 %s 删除文档 %s 成功",endpoint,docId)
			}
		}(endpoint)
	}
	wg.Wait()
	return int(atomic.LoadInt32(&n))
}
