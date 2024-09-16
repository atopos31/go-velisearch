package indexservice

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"sync/atomic"

	kvdb "github.com/atopos31/go-velisearch/internal/kv_db"
	reverseindex "github.com/atopos31/go-velisearch/internal/reverse_index"
	"github.com/atopos31/go-velisearch/types"
	"github.com/atopos31/go-velisearch/util"
)

type Indexer struct {
	forwardIndex kvdb.KeyValueDB
	reverseIndex reverseindex.IRverseIndexer
	maxIntId     uint64
}

func (indexer *Indexer) Init(DocNumEstimate int, dbType int, dataDir string) error {
	forwardIndex, err := kvdb.GetKVdb(dbType, dataDir)
	if err != nil {
		return err
	}
	reverseIndex := reverseindex.NewSkipListInvertedIndexer(DocNumEstimate)

	indexer.forwardIndex = forwardIndex
	indexer.reverseIndex = reverseIndex
	return nil
}

func (indexer *Indexer) Close() error {
	return indexer.forwardIndex.Close()
}

func (indexer *Indexer) AddDoc(doc types.Document) (int, error) {
	docId := strings.TrimSpace(doc.Id)
	if len(docId) == 0 {
		return 0, fmt.Errorf("doc id cannot be empty")
	}

	// 删除doc 如果他存在
	indexer.DeleteDoc(docId)

	// 为新文档自动生成一个唯一的IntId
	doc.IntId = atomic.AddUint64(&indexer.maxIntId, 1)

	var value bytes.Buffer
	encoder := gob.NewEncoder(&value) // 创建一个编码器 将数据结构编码为字节流
	if err := encoder.Encode(doc); err != nil {
		return 0, fmt.Errorf("error encoding doc: %v", err)
	}

	if err := indexer.forwardIndex.Set([]byte(docId), value.Bytes()); err != nil {
		return 0, fmt.Errorf("error adding doc to forward index: %v", err)
	}

	indexer.reverseIndex.Add(doc)

	return 1, nil
}

func (indexer *Indexer) DeleteDoc(docId string) int {
	if len(docId) == 0 {
		util.Log.Printf("doc id cannot be empty")
		return 0
	}

	forwardKey := []byte(docId)
	docBytes, err := indexer.forwardIndex.Get(forwardKey)
	if err != nil {
		util.Log.Printf("error getting doc from forward index: %v", err)
		return 0
	}

	// 正排索引中不存在
	if len(docBytes) == 0 {
		util.Log.Printf("doc not found in forward index")
		return 0
	}

	reader := bytes.NewReader(docBytes)
	var doc types.Document
	if err := gob.NewDecoder(reader); err != nil {
		util.Log.Printf("error unmarshaling doc: %v", err)
		return 0
	}

	// 对每一个keyword 删除倒排索引
	for _, keyword := range doc.Keywords {
		indexer.reverseIndex.Delete(doc.IntId, keyword)
	}

	// 删除正排索引
	if err := indexer.forwardIndex.Delete(forwardKey); err != nil {
		util.Log.Printf("error deleting doc from forward index: %v", err)
		return 0
	}

	return 1
}

// 从正排索引中 加载倒排索引到内存
func (indexer *Indexer) LoadFormIndexFile() int {
	reader := bytes.NewReader([]byte{})

	n, err := indexer.forwardIndex.IterDB(func(k, v []byte) error {
		reader.Reset(v) // 重置reader

		var doc types.Document
		decoder := gob.NewDecoder(reader)

		if err := decoder.Decode(&doc); err != nil {
			return fmt.Errorf("error decoding doc: %v", err)
		}

		indexer.AddDoc(doc)
		return nil
	})

	if err != nil {
		return 0
	}

	util.Log.Printf("loaded %d docs from index file", n)
	return int(n)
}

func (indexer *Indexer) Search(query *types.TermQuery, onFlag, offFlag uint64, orFlags []uint64) []*types.Document {
	docIds := indexer.reverseIndex.Search(query, onFlag, offFlag, orFlags)
	if len(docIds) == 0 {
		return nil
	}

	// 构建正排索引的关键字集合，用于批量获取文档
	keys := make([][]byte, 0, len(docIds))
	for _, docId := range docIds {
		keys = append(keys, []byte(docId))
	}

	docBytes, err := indexer.forwardIndex.BatchGet(keys)
	if err != nil {
		util.Log.Printf("从正排索引批量获取文档出错: %v", err)
		return nil
	}

	result := make([]*types.Document, 0, len(docIds))
	reader := bytes.NewReader([]byte{}) // 用于读取二进制数据的字节读取器
	for _, docByte := range docBytes {
		reader.Reset(docByte)             // 重置读取器
		decoder := gob.NewDecoder(reader) // 创建Gob解码器
		var doc types.Document
		err = decoder.Decode(&doc) // 解码文档
		if err == nil {
			result = append(result, &doc) // 将解码后的文档添加到结果集中
		}
	}
	return result
}

func (indexer *Indexer) Count() int {
	n, err := indexer.forwardIndex.IterKey(func(k []byte) error {
		return nil
	})
	if err != nil {
		util.Log.Printf("遍历键时出错: %v", err)
		return 0
	}
	return int(n)
}
