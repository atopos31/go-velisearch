package indexservice

import types "github.com/atopos31/go-velisearch/types"

// Indexer Sentinel（分布式grpc的哨兵）和 Indexer_worker（单机索引）都实现了该接口
type Iindexer interface {
	AddDoc(doc types.Document) (int, error)
	DeleteDoc(docId string) int
	Search(query *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []*types.Document
	Count() int
	Close() error
}