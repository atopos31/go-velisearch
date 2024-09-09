package kvdb

import (
	"os"
	"strings"

	"github.com/atopos31/go-velisearch/util"
)

// db type
const (
	BOLT = iota
	BADGER
)

// 正排索引KeyValueDB
type KeyValueDB interface {
	Open() error                                      // 初始化DB
	GetDbPath() string                                // 获取存储数据的目录
	Set(k, v []byte) error                            // 写入<key, value>
	BatchSet(keys, values [][]byte) error             // 批量写入<key, value>
	Get(k []byte) ([]byte, error)                     // 读取key对应的value
	BatchGet(keys [][]byte) ([][]byte, error)         // 批量读取，注意不保证顺序
	Delete(k []byte) error                            // 删除
	BatchDelete(keys [][]byte) error                  // 批量删除
	Has(k []byte) bool                                // 判断某个key是否存在
	IterDB(fn func(k, v []byte) error) (int64, error) // 遍历数据库，返回数据的条数
	IterKey(fn func(k []byte) error) (int64, error)   // 遍历所有key，返回数据的条数
	Close() error                                     // 把内存中的数据持久化到磁盘，同时释放文件锁
}

func GetKVdb(dbType int, path string) (KeyValueDB, error) {
	paths := strings.Split(path, "/")
	parentPath := strings.Join(paths[:len(paths)-1], "/")
	stat, err := os.Stat(parentPath) // 获取父目录信息

	if os.IsNotExist(err) {
		util.Log.Printf("parentPath not exit create dir: %s", parentPath)
		if err := os.MkdirAll(parentPath, os.ModePerm); err != nil {
			return nil, err
		}
	} else {
		if stat.Mode().IsRegular() {
			util.Log.Printf("%s is a regular file, will delete it", parentPath)
			if err := os.Remove(parentPath); err != nil {
				return nil, err
			}
		}

		if err := os.MkdirAll(parentPath, os.ModePerm); err != nil {
			return nil, err
		}

	}
	var db KeyValueDB
	switch dbType {
	case BADGER:
		// db = new(Badger).WithDataPath(path)
	default:
		// 默认使用 Bolt 数据库，并设置相应的桶
		db = new(Bbolt).WithDataPath(path).WithBucket("vrlisearch")
	}

	err = db.Open()
	return db, err
}
