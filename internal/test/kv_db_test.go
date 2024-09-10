package test

import (
	"testing"

	kvdb "github.com/atopos31/go-velisearch/internal/kv_db"
)

var (
	db       kvdb.KeyValueDB // 面向接口测试
	setup    func()          // 测试前初始化
	teardown func()          // 测试后清理
)

func init() {
	teardown = func() {
		db.Close()
	}
}

func TestKVDB(t *testing.T) {
	setup = func() {
		db, _ = kvdb.GetKVdb(kvdb.BADGER, "D:/golearnprodevbox/boltdb")
	}

	t.Run("TestSet", testSet)
	t.Run("TestGet", testGet)
	t.Run("TestBatchSet", testBatchSet)
	t.Run("TestBatchGet", testBatchGet)
	t.Run("TestDelete", testDelete)
}

func testSet(t *testing.T) {
	setup()
	defer teardown()

	db.Set([]byte("key"), []byte("value"))
	if !db.Has([]byte("key")) {
		t.Errorf("key not found")
	}
}

func testGet(t *testing.T) {
	setup()
	defer teardown()

	db.Set([]byte("key"), []byte("value"))
	res, err := db.Get([]byte("key"))
	if err != nil {
		t.Errorf("get error")
	}
	if string(res) != "value" {
		t.Errorf("get error")
	}
}

func testBatchSet(t *testing.T) {
	setup()
	defer teardown()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	db.BatchSet(keys, values)
	for i, key := range keys {
		if !db.Has(key) {
			t.Errorf("key not found")
		}
		res, err := db.Get(key)
		if err != nil {
			t.Errorf("get error")
		}
		if string(res) != string(values[i]) {
			t.Errorf("get error")
		}
	}
}

func testBatchGet(t *testing.T) {
	setup()
	defer teardown()

	keys := [][]byte{[]byte("key1"), []byte("key2"), []byte("key3")}
	values := [][]byte{[]byte("value1"), []byte("value2"), []byte("value3")}
	db.BatchSet(keys, values)
	res, err := db.BatchGet(keys)
	if err != nil {
		t.Errorf("get error")
	}
	for i, _ := range keys {
		if string(res[i]) != string(values[i]) {
			t.Errorf("get error")
		}
	}
}

func testDelete(t *testing.T) {
	setup()
	defer teardown()

	db.Set([]byte("key"), []byte("value"))
	db.Delete([]byte("key"))
	if db.Has([]byte("key")) {
		t.Errorf("key found")
	}
}


