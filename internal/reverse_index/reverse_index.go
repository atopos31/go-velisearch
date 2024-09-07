package reverseindex

import "github.com/atopos31/go-velisearch/types"

type IRverseIndexer interface {
	Add(doc *types.Document)
	Delete(IntId uint64, keyword *types.Keyword)
}
