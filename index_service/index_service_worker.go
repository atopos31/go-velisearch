package indexservice

import (
	context "context"
	fmt "fmt"
	"strconv"
	"time"

	servicehub "github.com/atopos31/go-velisearch/index_service/service_hub"
	types "github.com/atopos31/go-velisearch/types"
	"github.com/atopos31/go-velisearch/util"
)

const IndexService = "index_service"

type IndexServiceWorker struct {
	indexer  *Indexer                   // 正排索引和倒排索引的结合
	hub      *servicehub.EtcdServiceHub // 服务发现 注册 注销
	selfAddr string                     // 本地地址
}

// 初始化
func (w *IndexServiceWorker) Init(DocNumEstimate int, dbtype int, DataDir string) error {
	w.indexer = new(Indexer)
	return w.indexer.Init(DocNumEstimate, dbtype, DataDir)
}

func (w *IndexServiceWorker) RegisterService(etcdServers []string,servicePort int) error {
	if len(etcdServers) >0 {
		if servicePort <=1024 || servicePort > 65535 {
			return fmt.Errorf("service port must be between 1024 and 65535")
		}

		// 获取本地IP地址
		localIP, err := util.GetLocalIP()
		if err != nil {
			return fmt.Errorf("获取本地IP地址失败: %v", err)
		}

		// 单机模式下，将本地IP写死为127.0.0.1
		localIP = "127.0.0.1"
		w.selfAddr = localIP + ":" + strconv.Itoa(servicePort)

		var heartbeatFrequency int64 = 3

		hub := servicehub.GetServiceHub(etcdServers, heartbeatFrequency)

		leaseID , err := hub.RegisterService(IndexService, w.selfAddr, 0)
		if err != nil {
			return fmt.Errorf("注册服务失败: %v", err)
		}

		w.hub = hub


		go func() {
			for {
				_, err := hub.RegisterService(IndexService, w.selfAddr, leaseID)
				if err != nil {
					util.Log.Printf("续约服务租约失败，租约ID: %v, 错误: %v", leaseID, err)
				}
				// 心跳间隔时间稍短于最大超时时间
				time.Sleep(time.Duration(heartbeatFrequency)*time.Second - 100*time.Millisecond)
			}
		}()

	}

	return nil
}

func (w *IndexServiceWorker) LoadFromIndexFile() int {
	return w.indexer.LoadFormIndexFile()
}

func (w *IndexServiceWorker) Close() error {
	// 检查是否需要注销服务
	if w.hub != nil {
		// 注销服务
		err := w.hub.UnregisterService(IndexService, w.selfAddr)
		if err != nil {
			util.Log.Printf("注销服务失败，服务地址: %v, 错误: %v", w.selfAddr, err)
			return err
		}
		util.Log.Printf("注销服务成功，服务地址: %v", w.selfAddr)
	}

	// 关闭索引
	return w.indexer.Close()
}

func (w *IndexServiceWorker) DeleteDoc(ctx context.Context, docId *DocId) (*AffectedCount, error) {
	// 调用Indexer的DeleteDoc方法删除文档，并返回影响的文档数量
	return &AffectedCount{
		Count: int32(w.indexer.DeleteDoc(docId.DocId)),
	}, nil
}

func (w *IndexServiceWorker) AddDoc(ctx context.Context, doc *types.Document) (*AffectedCount, error) {
	// 调用Indexer的AddDoc方法添加文档，并返回影响的文档数量
	n, err := w.indexer.AddDoc(*doc)
	return &AffectedCount{
		Count: int32(n),
	}, err
}

func (w *IndexServiceWorker) Search(ctx context.Context, request *SearchRequest) (*SearchResult, error) {
	// 调用Indexer的Search方法进行检索，并返回检索结果
	result := w.indexer.Search(request.Query, request.OnFlag, request.OffFlag, request.OrFlags)
	return &SearchResult{
		Results: result,
	}, nil
}

func (w *IndexServiceWorker) Count(ctx context.Context, request *CountRequest) (*AffectedCount, error) {
	// 调用Indexer的Count方法获取文档数量，并返回结果
	return &AffectedCount{
		Count: int32(w.indexer.Count()),
	}, nil
}
