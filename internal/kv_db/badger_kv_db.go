package kvdb

import (
	"errors"
	"os"
	"path"
	"sync/atomic"

	"github.com/atopos31/go-velisearch/util"
	"github.com/dgraph-io/badger/v4"
)

type Badger struct {
	db   *badger.DB
	path string
}

// Open 初始化数据库
func (b *Badger) Open() error {
	DataDir := b.GetDbPath()
	// 如果DataDir对应的文件夹已存在则什么都不做，如果DataDir对应的文件已存在则返回错误
	if err := os.MkdirAll(path.Dir(DataDir), os.ModePerm); err != nil {
		return err
	}
	// Builder模式，可以连续使用多个With()函数来构造对象
	option := badger.DefaultOptions(DataDir).WithNumVersionsToKeep(1).WithLoggingLevel(badger.ERROR)
	// 文件只能被一个进程使用，如果不调用Close则下次无法Open。手动释放锁的办法：把LOCK文件删掉
	db, err := badger.Open(option)
	if err != nil {
		return err
	} else {
		b.db = db
		return nil
	}
}

// GetDbPath 获取数据库文件的路径
func (b *Badger) GetDbPath() string {
	return b.path
}

// Set 写入<key, value>，为单个写操作开一个事务
func (b *Badger) Set(k, v []byte) error {
	// db.Update相当于打开了一个读写事务:db.NewTransaction(true)。用db.Update的好处在于不用显式调用Txn.Commit()
	err := b.db.Update(func(txn *badger.Txn) error {
		// duration是能存活的时长
		// duration := time.Hour * 87600
		return txn.Set(k, v)
	})
	return err
}

// BatchSet 批量写入<key, value>，多个写操作使用一个事务
func (b *Badger) BatchSet(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.New("keys and values do not match")
	}
	txn := b.db.NewTransaction(true)
	for i, key := range keys {
		value := values[i]
		// duration := time.Hour * 87600
		// util.util.Log.Debugf("duration",duration)
		if err := txn.Set(key, value); err != nil {
			// 发生异常时就提交老事务，然后开一个新事务，重试set
			_ = txn.Commit()
			txn = b.db.NewTransaction(true)
			_ = txn.Set(key, value)
		}
	}
	err := txn.Commit()
	return err
}

// Get 读取key对应的value，如果key不存在会返回error: Key not found
func (b *Badger) Get(k []byte) ([]byte, error) {
	var v []byte
	// db.View相当于打开了一个读写事务:db.NewTransaction(true)。用db.Update的好处在于不用显式调用Txn.Discard()
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		// buffer := make([]byte, badgerOptions.ValueLogMaxEntries)
		// v, err = item.ValueCopy(buffer) //item只能在事务内部使用，如果要在事务外部使用需要通过ValueCopy
		err = item.Value(func(val []byte) error {
			v = val
			return nil
		})
		return err
	})
	return v, err
}

// BatchGet 批量读取，返回的values与传入的keys顺序保持一致。如果key不存在或读取失败则对应的value是空数组
func (b *Badger) BatchGet(keys [][]byte) ([][]byte, error) {
	var err error
	txn := b.db.NewTransaction(false) //只读事务
	values := make([][]byte, len(keys))
	for i, key := range keys {
		var item *badger.Item
		item, err = txn.Get(key)
		if err == nil {
			// buffer := make([]byte, badgerOptions.ValueLogMaxEntries)
			var v []byte
			// v, err = item.ValueCopy(buffer)
			err = item.Value(func(val []byte) error {
				v = val
				return nil
			})
			if err == nil {
				values[i] = v
			} else {
				// 拷贝失败，把value设为空数组
				values[i] = []byte{}
			}
		} else {
			// 读取失败，把value设为空数组
			values[i] = []byte{}
			// 如果真的发生异常，则开一个新事务继续读后面的key
			if !errors.Is(err, badger.ErrKeyNotFound) {
				txn.Discard()
				txn = b.db.NewTransaction(false)
			}
		}
	}
	// 只读事务调Discard就可以了，不需要调Commit。Commit内部也会调Discard
	txn.Discard()
	return values, err
}

// Delete 删除
func (b *Badger) Delete(k []byte) error {
	err := b.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(k)
	})
	return err
}

// BatchDelete 批量删除
func (b *Badger) BatchDelete(keys [][]byte) error {
	txn := b.db.NewTransaction(true)
	for _, key := range keys {
		if err := txn.Delete(key); err != nil {
			_ = txn.Commit()
			// 发生异常时就提交老事务，然后开一个新事务，重试delete
			txn = b.db.NewTransaction(true)
			_ = txn.Delete(key)
		}
	}
	err := txn.Commit()
	return err
}

// Has 判断某个key是否存在
func (b *Badger) Has(k []byte) bool {
	var exists = false
	// db.View相当于打开了一个读写事务:db.NewTransaction(true)。用db.Update的好处在于不用显式调用Txn.Discard()
	err := b.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(k)
		if err != nil {
			return err
		} else {
			exists = true // 没有任何异常发生，则认为k存在。如果k不存在会发生ErrKeyNotFound
		}
		return err
	})
	if err != nil {
		return false
	}
	return exists
}

// IterDB 遍历数据库，返回数据的条数
func (b *Badger) IterDB(fn func(k, v []byte) error) (int64, error) {
	var count int64
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			key := item.Key()

			var v []byte
			// var err error
			// buffer := make([]byte, badgerOptions.ValueLogMaxEntries)
			// v, err = item.ValueCopy(buffer)
			err := item.Value(func(val []byte) error {
				v = val
				return nil
			})
			if err != nil {
				continue
			}
			if err := fn(key, v); err == nil {
				atomic.AddInt64(&count, 1)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return atomic.LoadInt64(&count), nil
}

// IterKey 只遍历key。key是全部存在LSM tree上的，只需要读内存，所以很快
func (b *Badger) IterKey(fn func(k []byte) error) (int64, error) {
	var total int64
	err := b.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		// 只需要读key，所以把PrefetchValues设为false
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := fn(k); err == nil {
				atomic.AddInt64(&total, 1)
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return atomic.LoadInt64(&total), nil
}

// Close 关闭数据库，把内存中的数据flush到磁盘，同时释放文件锁
func (b *Badger) Close() error {
	return b.db.Close()
}

// WithDataPath 方法设置 Badger 结构的本地存储目录路径。
func (b *Badger) WithDataPath(path string) *Badger {
	b.path = path
	return b
}

func (b *Badger) CheckAndGC() {
	lsmSize1, vlogSize1 := b.db.Size()
	for {
		if err := b.db.RunValueLogGC(0.5); err == badger.ErrNoRewrite || err == badger.ErrRejected {
			break
		}
	}
	lsmSize2, vlogSize2 := b.db.Size()
	if vlogSize2 < vlogSize1 {
		util.Log.Printf("badger before GC, LSM %d, vlog %d. after GC, LSM %d, vlog %d", lsmSize1, vlogSize1, lsmSize2, vlogSize2)
	} else {
		util.Log.Printf("collect zero garbage")
	}
}
