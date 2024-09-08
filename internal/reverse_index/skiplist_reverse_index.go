package reverseindex

import (
	"runtime"
	"sync"

	"github.com/atopos31/go-velisearch/types"
	"github.com/atopos31/go-velisearch/util"
	"github.com/huandu/skiplist"
	farmhash "github.com/leemcloughlin/gofarmhash"
)

var _ IRverseIndexer = (*SkipListInvertedIndexer)(nil)

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

func (indexer *SkipListInvertedIndexer) Delete(IntId uint64, keyword *types.Keyword) {
	Key := keyword.ToString()
	lock := indexer.getLock(Key)
	lock.Lock()
	defer lock.Unlock()
	if value, exist := indexer.table.Get(Key); exist {
		list := value.(*skiplist.SkipList)
		list.Remove(IntId)
	}
}

// Search 根据查询条件搜索 invert index
func (indexer *SkipListInvertedIndexer) Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string {
	result := indexer.search(query, onFlag, offFlag, orFlags)
	if result == nil {
		return nil
	}

	arr := make([]string, 0, result.Len())

	node := result.Front()
	for node != nil {
		skipListValue := node.Value.(SkipListValue)
		arr = append(arr, skipListValue.Id)
		node = node.Next()
	}

	return arr
}

func (indexer *SkipListInvertedIndexer) search(q *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) *skiplist.SkipList {
	switch {
	case q.Keyword != nil:
		keyword := q.Keyword.ToString()

		if value, exists := indexer.table.Get(keyword); exists {
			list := value.(*skiplist.SkipList)
			result := skiplist.New(skiplist.Uint64)

			node := list.Front()
			for node != nil {
				intId := node.Key().(uint64)
				skipListValue := node.Value.(SkipListValue)
				flag := skipListValue.BitsFeature
				if intId > 0 && indexer.FilterByBits(flag, onFlag, offFlag, orFlags) {
					result.Set(intId, skipListValue)
				}
				node = node.Next()
			}
		}

	case len(q.Must) > 0:
		results := make([]*skiplist.SkipList, 0, len(q.Must))
		for _, query := range q.Must {
			results = append(results, indexer.search(query, onFlag, offFlag, orFlags))
		}
		// 计算 Must 查询结果的交集
		return IntersectionOfSkipLists(results...)

	case len(q.Should) > 0:
		results := make([]*skiplist.SkipList, 0, len(q.Should))
		for _, query := range q.Should {
			results = append(results, indexer.search(query, onFlag, offFlag, orFlags))
		}
		return IntersectionOfSkipLists(results...)
	}

	return nil
}

// FilterByBits 根据 bits 特征进行过滤。
func (indexer SkipListInvertedIndexer) FilterByBits(bits, onFlag, offFlag uint64, orFlags []uint64) bool {
	if bits&onFlag != onFlag {
		return false
	}
	if bits&offFlag != uint64(0) {
		return false
	}
	for _, orFlag := range orFlags {
		if orFlag > 0 && bits&orFlag <= 0 {
			return false
		}
	}
	return true
}

func (indexer *SkipListInvertedIndexer) getLock(key string) *sync.RWMutex {
	n := int(farmhash.Hash32WithSeed([]byte(key), 0))
	return &indexer.locks[n%len(indexer.locks)]
}

// IntersectionOfSkipLists 计算多个 SkipList 的交集。
func IntersectionOfSkipLists(lists ...*skiplist.SkipList) *skiplist.SkipList {
	if len(lists) == 0 {
		return nil
	}
	if len(lists) == 1 {
		return lists[0]
	}

	result := skiplist.New(skiplist.Uint64)
	currNodes := make([]*skiplist.Element, len(lists))
	for i, list := range lists {
		if list == nil || list.Len() == 0 {
			return nil // 只要有链表为空，则返回 nil
		}
		currNodes[i] = list.Front() // 初始化每个跳表当前节点
	}

	for {
		maxList := make(map[int]struct{}, len(currNodes))
		var maxValue uint64 = 0
		for i, node := range currNodes {
			if node.Value.(uint64) > maxValue {
				maxValue = node.Value.(uint64)
				maxList = make(map[int]struct{})
				maxList[i] = struct{}{}
			} else if node.Value.(uint64) == maxValue {
				maxList[i] = struct{}{}
			}
		}
		if len(maxList) == len(currNodes) {
			result.Set(currNodes[0].Key(), currNodes[0].Value)
			for i, node := range currNodes {
				currNodes[i] = node.Next()
				if currNodes[i] == nil {
					return result
				}
			}
		} else {
			for i, node := range currNodes {
				if _, ok := maxList[i]; !ok {
					currNodes[i] = node.Next()
					if currNodes[i] == nil {
						return result
					}
				}
			}
		}
	}
}
