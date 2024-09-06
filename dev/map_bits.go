package dev

import (
	"github.com/atopos31/go-velisearch/dev/util"
	"github.com/huandu/skiplist"
)

type BitMap struct {
	Table uint64
}

func CreatBitMap(min int, arr []int) *BitMap {
	bitMap := new(BitMap)
	for _, ele := range arr {
		bitMap.Table = util.SetBit1(bitMap.Table, ele-min)
	}
	return bitMap
}

// 位图求交集
func IntersectionOfBitMap(a, b *BitMap, min int) []int {
	react := make([]int, 0, 64)
	s := a.Table & b.Table
	for i := 1; i <= 64; i++ {
		if util.IsBit1(s, i) {
			react = append(react, i+min)
		}
	}
	return react
}

// 两个有序链表求交集
func IntersectionOfOrderedList(a, b []int) []int {
	m, n := len(a), len(b)
	if m == 0 || n == 0 {
		return nil
	}
	rect := make([]int, 0, max(m, n)) // 默认长度为两个链表的较大长度
	var i, j int                      // 指针
	for i < m && j < n {
		if a[i] == b[j] {
			rect = append(rect, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return rect
}

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
