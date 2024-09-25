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

// 检查是否实现
var _ Iindexer = (*Sentinel)(nil)

// 对外提供服务
type Sentinel struct {
	hub      servicehub.ServiceHub // 发现服务
	connPool sync.Map              // 缓存与Worker的连接
}

func NewSentinel(etcdServers []string) *Sentinel {
	return &Sentinel{
		hub:      servicehub.GetServiceHubProxy(etcdServers, 3, 100),
		connPool: sync.Map{},
	}
}

func (s *Sentinel) GetGrpcConn(endpoint string) *grpc.ClientConn {
	v, exist := s.connPool.Load(endpoint)
	if exist {
		conn := v.(*grpc.ClientConn)

		state := conn.GetState()
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			// 连接不可用
			util.Log.Printf("连接到 endpoint %s 的状态为 %s", endpoint, state.String())
			conn.Close()
			s.connPool.Delete(endpoint)
		} else {
			return conn
		}

	}

	cts, cancel := context.WithTimeout(context.Background(), 200*time.Microsecond)
	defer cancel()

	grpcConn, err := grpc.DialContext(cts, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock()) // 阻塞等待连接
	if err != nil {
		util.Log.Printf("连接 %s 失败: %s", endpoint, err)
		return nil
	}

	util.Log.Printf("连接 %s 成功", endpoint)
	s.connPool.Store(endpoint, grpcConn)
	return grpcConn
}

func (s *Sentinel) AddDoc(doc types.Document) (int, error) {
	endpoint := s.hub.GetServiceEndpoint(IndexService)
	if len(endpoint) == 0 {
		return 0, fmt.Errorf("未找到服务%s 的有效节点", IndexService)
	}

	conn := s.GetGrpcConn(endpoint)
	if conn == nil {
		return 0, fmt.Errorf("无法连接到 %s", endpoint)
	}

	client := NewIndexServiceClient(conn)
	affetcd, err := client.AddDoc(context.Background(), &doc)
	if err != nil {
		return 0, err
	}
	util.Log.Printf("向 %s 添加文档 %s 成功", endpoint, doc.Id)
	return int(affetcd.Count), nil
}

// 删除时 需要在所有的节点上删除
func (s *Sentinel) DeleteDoc(docId string) int {
	endpoints := s.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return 0
	}

	var n int32
	wg := sync.WaitGroup{}
	wg.Add(len(endpoints))
	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			conn := s.GetGrpcConn(endpoint)
			if conn == nil {
				util.Log.Printf("无法连接到 %s", endpoint)
				return
			}
			client := NewIndexServiceClient(conn)
			affetcd, err := client.DeleteDoc(context.Background(), &DocId{docId})
			if err != nil {
				util.Log.Printf("向 %s 删除文档 %s 失败: %s", endpoint, docId, err)
				return
			}
			if affetcd.Count > 0 {
				atomic.AddInt32(&n, affetcd.Count)
				util.Log.Printf("向 %s 删除文档 %s 成功", endpoint, docId)
			}
		}(endpoint)
	}
	wg.Wait()
	return int(atomic.LoadInt32(&n))
}

func (s *Sentinel) Search(query *types.TermQuery, onFlag, offFlag uint64, orFlags []uint64) []*types.Document {
	endpoints := s.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return nil
	}

	docs := make([]*types.Document, 0, 1000)
	resultChan := make(chan *types.Document, 1000)

	var wg sync.WaitGroup
	wg.Add(len(endpoints))

	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			conn := s.GetGrpcConn(endpoint)
			if conn == nil {
				util.Log.Printf("无法连接到 %s", endpoint)
				return
			}
			client := NewIndexServiceClient(conn)

			searchResult, err := client.Search(context.Background(), &SearchRequest{query, onFlag, offFlag, orFlags})
			if err != nil {
				util.Log.Printf("向 %s 发送查询请求失败: %s", endpoint, err)
				return
			}
			if len(searchResult.Results) > 0 {
				util.Log.Printf("向 %s 发送查询请求成功，返回 %d 条结果", endpoint, len(searchResult.Results))
				for _, doc := range searchResult.Results {
					resultChan <- doc
				}
			}
		}(endpoint)
	}

	signalChan := make(chan bool)
	go func() {
		for doc := range resultChan {
			docs = append(docs, doc)
		}

		signalChan <- true
	}()

	wg.Wait()         // 等待所有goroutine完成
	close(resultChan) // 关闭结果通道 当resultChan关闭时，上面协程range循环结束
	<-signalChan      // 等待结果处理完成
	return docs
}

func (s *Sentinel) Count() int {
	endpoints := s.hub.GetServiceEndpoints(IndexService)
	if len(endpoints) == 0 {
		return 0
	}

	var count int32
	var wg sync.WaitGroup
	wg.Add(len(endpoints))

	for _, endpoint := range endpoints {
		go func(endpoint string) {
			defer wg.Done()
			conn := s.GetGrpcConn(endpoint)
			if conn == nil {
				util.Log.Printf("无法连接到 %s", endpoint)
				return
			}
			client := NewIndexServiceClient(conn)
			countResult, err := client.Count(context.Background(), &CountRequest{})
			if err != nil {
				util.Log.Printf("向 %s 发送查询请求失败: %s", endpoint, err)
				return
			}
			if countResult.Count > 0 {
				atomic.AddInt32(&count, countResult.Count)
				util.Log.Printf("向 %s 发送查询请求成功，返回 %d 条结果", endpoint, countResult.Count)
			}
		}(endpoint)
	}
	wg.Wait()
	return int(atomic.LoadInt32(&count))
}

func (sentinel *Sentinel) Close() (err error) {
	sentinel.connPool.Range(func(key, value any) bool {
		conn := value.(*grpc.ClientConn)
		err = conn.Close()
		return true
	})
	sentinel.hub.Close()
	return
}
