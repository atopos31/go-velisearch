package reverseindex

import "github.com/atopos31/go-velisearch/types"

type IRverseIndexer interface {
	Add(doc *types.Document)
	// keyword定位到某个value IntId在value的链式结构中确定位置
	Delete(IntId uint64, keyword *types.Keyword)
	// onFlag：所有 bits 必须完全匹配 offFlag所有 bits 必须完全不匹配 orFlags：bits 必须匹配 `orFlags` 列表中的所有标志中的至少一个。
	Search(q *types.TermQuery, onFlag uint64, offFlag uint64, orFlags []uint64) []string
}
