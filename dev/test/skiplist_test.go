package test

import (
	"testing"

	"github.com/huandu/skiplist"
)

func TestSkipList(t *testing.T) {
	list := skiplist.New(skiplist.Int32)
	list.Set(1, 1)
	list.Set(2, 2)
	list.Set(3, 3)

	node := list.Front()
	for node != nil {
		t.Log(node.Key(), node.Value)
		node = node.Next()
	}
}
