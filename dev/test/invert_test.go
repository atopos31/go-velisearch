package test

import (
	"fmt"
	"testing"

	"github.com/atopos31/go-velisearch/dev"
)

func TestBuildInvertIndex(t *testing.T) {
	docs := []*dev.Doc{
		{Id: 1, Keywords: []string{"go", "数据结构", "链表"}},
		{Id: 2, Keywords: []string{"java", "spring", "框架", "数据结构"}},
		{Id: 3, Keywords: []string{"数据库", "go", "数据结构"}},
	}
	invertIndex := dev.BuildInvertIndex(docs)
	for keyword, docIds := range invertIndex {
		fmt.Printf("%s: %v\n", keyword, docIds)
	}
}
