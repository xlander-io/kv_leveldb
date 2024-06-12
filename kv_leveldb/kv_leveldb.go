package kv_leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/xlander-io/kv/kv_interface"
)

var Default_W_OP_TRUE = &opt.WriteOptions{
	Sync: true,
}

var Default_W_OP_FALSE = &opt.WriteOptions{
	Sync: false,
}

type KV_LEVELDB struct {
	leveldb *leveldb.DB
}

func NewDB(db_path string) (kv_interface.KVDB, error) {
	level_db_, err := leveldb.OpenFile(db_path, nil)
	if err != nil {
		return nil, err
	}

	return &KV_LEVELDB{leveldb: level_db_}, nil
}

func (db *KV_LEVELDB) Close() error {
	return db.leveldb.Close()
}

func (db *KV_LEVELDB) Put(key, value []byte, sync bool) error {
	if sync {
		return db.leveldb.Put(key, value, Default_W_OP_TRUE)
	} else {
		return db.leveldb.Put(key, value, Default_W_OP_FALSE)
	}
}

func (db *KV_LEVELDB) Delete(key []byte, sync bool) error {
	if sync {
		return db.leveldb.Delete(key, Default_W_OP_TRUE)
	} else {
		return db.leveldb.Delete(key, Default_W_OP_FALSE)
	}
}

func (db *KV_LEVELDB) Get(key []byte) (value []byte, err error) {
	return db.leveldb.Get(key, nil)
}

func (db *KV_LEVELDB) WriteBatch(batch *kv_interface.Batch, sync bool) error {
	leveldb_batch := new(leveldb.Batch)
	batch.Loop(func(key, val []byte) {
		leveldb_batch.Put(key, val)
	})

	if sync {
		return db.leveldb.Write(leveldb_batch, Default_W_OP_TRUE)
	} else {
		return db.leveldb.Write(leveldb_batch, Default_W_OP_FALSE)
	}
}

func (db *KV_LEVELDB) NewIterator(start []byte, limit []byte) kv_interface.Iterator {
	return db.leveldb.NewIterator(&util.Range{Start: start, Limit: limit}, nil)
}
