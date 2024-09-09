package kvdb

import (
	"errors"
	"sync/atomic"

	"go.etcd.io/bbolt"
)

var NoDataErr = errors.New("no data")

type Bbolt struct {
	db     *bbolt.DB
	path   string // 数据库文件路径
	bucket []byte
}

func (bb *Bbolt) WithDataPath(path string) *Bbolt {
	bb.path = path
	return bb
}

func (bb *Bbolt) WithBucket(bucket string) *Bbolt {
	bb.bucket = []byte(bucket)
	return bb
}

func (bb *Bbolt) Open() error {
	dataDir := bb.GetDbPath()
	db, err := bbolt.Open(dataDir, 0600, bbolt.DefaultOptions)
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bb.bucket)
		return err
	})
	if err != nil {
		db.Close()
		return err
	} else {
		bb.db = db
	}
	return nil
}

func (bb *Bbolt) Close() error {
	return bb.db.Close()
}

func (bb *Bbolt) GetDbPath() string {
	return bb.path
}

func (bb *Bbolt) Get(k []byte) ([]byte, error) {
	var ival []byte
	err := bb.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		ival = bucket.Get(k)
		return nil
	})
	if len(ival) == 0 {
		return nil, NoDataErr
	}
	return ival, err
}

func (bb *Bbolt) Set(k, v []byte) error {
	return bb.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		return bucket.Put(k, v)
	})
}

func (bb *Bbolt) BatchGet(keys [][]byte) ([][]byte, error) {
	var result [][]byte
	err := bb.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		for _, k := range keys {
			v := bucket.Get(k)
			result = append(result, v)
		}
		return nil
	})
	return result, err
}

func (bb *Bbolt) BatchSet(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.New("keys and values length not equal")
	}
	return bb.db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		for i := range keys {
			err := bucket.Put(keys[i], values[i])
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (bb *Bbolt) BatchDelete(keys [][]byte) error {
	return bb.db.Batch(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		for _, k := range keys {
			err := bucket.Delete(k)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (bb *Bbolt) Delete(k []byte) error {
	return bb.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		return bucket.Delete(k)
	})
}

func (bb *Bbolt) Has(k []byte) bool {
	var b []byte
	err := bb.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		b = bucket.Get(k)
		return nil
	})
	if err != nil || len(b) == 0 {
		return false
	} else {
		return true
	}
}

func (bb *Bbolt) IterKey(fn func(k []byte) error) (int64, error) {
	var total int64
	err := bb.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		c := bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			err := fn(k)
			if err != nil {
				return err
			}
			atomic.AddInt64(&total, 1)
		}
		return nil
	})
	return total, err
}

func (bb *Bbolt) IterDB(fn func(k, v []byte) error) (int64, error) {
	var total int64
	err := bb.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket(bb.bucket)
		c := bucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			err := fn(k, v)
			if err != nil {
				return err
			}
			atomic.AddInt64(&total, 1)
		}
		return nil
	})
	return total, err
}
