package reverseindex

import (
	"runtime"
	"sync"

	"github.com/atopos31/go-velisearch/types"
	"github.com/atopos31/go-velisearch/util"
	"github.com/huandu/skiplist"
	farmhash "github.com/leemcloughlin/gofarmhash"
)

type SkipListInvertedIndexer struct {
	table *util.ConcurrentHashMap // 使用分段锁保护的并发安全 map，用于存储倒排索引的数据
	locks []sync.RWMutex          // 针对相同的 key 进行竞争的锁，避免多个协程Add时数据丢失
}

type SkipListValue struct {
	Id          string // 业务侧的ID
	BitsFeature uint64 // 文件属性位图
}

func NewSkipListInvertedIndexer(docNumEstimate int) *SkipListInvertedIndexer {
	return &SkipListInvertedIndexer{
		// 小map数根据机器性能 依据核心数设置
		table: util.NewConcurrentHashMap(runtime.NumCPU(), docNumEstimate),
		locks: make([]sync.RWMutex, 1000),
	}
}

func (indexer *SkipListInvertedIndexer) Add(doc types.Document) {
	for _, keyword := range doc.Keywords {
		Key := keyword.ToString()
		lock := indexer.getLock(Key)
		skipListValue := SkipListValue{
			Id:          doc.Id,
			BitsFeature: doc.BitsFeature,
		}

		lock.Lock()
		if value, exist := indexer.table.Get(Key); exist {
			list := value.(*skiplist.SkipList)
			list.Set(doc.IntId, skipListValue)
		} else {
			list := skiplist.New(skiplist.Uint64)
			list.Set(doc.IntId, skipListValue)
			indexer.table.Set(Key, list)
		}
		lock.Unlock()
	}
}

func (indexer *SkipListInvertedIndexer) Delete(IntId uint64, keyword types.Keyword) {
	Key := keyword.ToString()
	lock := indexer.getLock(Key)
	lock.Lock()
	defer lock.Unlock()
	if value, exist := indexer.table.Get(Key); exist {
		list := value.(*skiplist.SkipList)
		list.Remove(IntId)
	}
}
// FilterByBits 根据 bits 特征进行过滤。
func (indexer SkipListInvertedIndexer) FilterByBits()


func (indexer *SkipListInvertedIndexer) getLock(key string) *sync.RWMutex {
	n := int(farmhash.Hash32WithSeed([]byte(key), 0))
	return &indexer.locks[n%len(indexer.locks)]
}
