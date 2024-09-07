package util

import (
	"sync"

	farmhash "github.com/leemcloughlin/gofarmhash"
	"golang.org/x/exp/maps"
)

type ConcurrentHashMap struct {
	mps  []map[string]any // 小map
	seg  int              // 小map数
	mus  []sync.RWMutex   // 小map锁 一人一把
	seed uint32           // 随机种子 用于执行farmhash
}

// cap 容量 seg 小map数
func NewConcurrentHashMap(seg int, cap int) *ConcurrentHashMap {
	mps := make([]map[string]any, seg)
	mus := make([]sync.RWMutex, seg)

	mcap := cap / seg // 每个小map的容量

	for i := 0; i < seg; i++ {
		mps[i] = make(map[string]any, mcap)
	}
	return &ConcurrentHashMap{
		mps:  mps,
		seg:  seg,
		mus:  mus,
		seed: 0,
	}
}

func (m *ConcurrentHashMap) Set(key string, value any) {
	segIndex := m.getSegIndex(key) // 获取小map索引
	m.mus[segIndex].Lock()         // 锁住
	defer m.mus[segIndex].Unlock() // 释放锁
	m.mps[segIndex][key] = value   // 赋值
}

func (m *ConcurrentHashMap) Get(key string) (any, bool) {
	segIndex := m.getSegIndex(key)
	m.mus[segIndex].RLock()
	defer m.mus[segIndex].RUnlock()
	value, ok := m.mps[segIndex][key]
	return value, ok
}

// 通过hsah获取小map索引
func (m *ConcurrentHashMap) getSegIndex(key string) int {
	hash := int(farmhash.Hash32WithSeed([]byte(key), m.seed))
	return hash % m.seg
}

// 创建一个迭代器
func (m *ConcurrentHashMap) CreatIterator() *ConcurrentHashMapIterator {
	keys := make([][]string, m.seg)
	for _, mp := range m.mps {
		row := maps.Keys(mp)
		keys = append(keys, row)
	}
	return &ConcurrentHashMapIterator{
		cm:       m,
		keys:     keys,
		rowIndex: 0,
		colIndex: 0,
	}
}

type MapEntry struct {
	key   string
	value any
}

// 迭代器Itertor模式接口 用来遍历一个map 返回key value结构体
type MapItertor interface {
	Next() *MapEntry
}

// 并发map迭代器的实现
type ConcurrentHashMapIterator struct {
	cm       *ConcurrentHashMap
	keys     [][]string
	rowIndex int // 行索引
	colIndex int // 列索引
}

func (iter *ConcurrentHashMapIterator) Next() *MapEntry {
	if iter.rowIndex >= len(iter.keys) {
		return nil
	}

	row := iter.keys[iter.rowIndex]
	if len(row) == 0 { // 本行为空
		iter.rowIndex++
		return iter.Next() // 递归到下一行
	}

	key := row[iter.colIndex]
	value, _ := iter.cm.Get(key)

	if iter.colIndex < len(row)-1 {
		iter.colIndex++
	} else {
		iter.rowIndex++
		iter.colIndex = 0
	}

	return &MapEntry{
		key:   key,
		value: value,
	}
}
