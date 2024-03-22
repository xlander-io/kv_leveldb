package test

import (
	"fmt"
	"log"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

func TestBasic(t *testing.T) {

	file_path := "./db"
	db, err := leveldb.OpenFile(file_path, nil)
	if err != nil {
		log.Fatal("init err:", err)
	}
	defer db.Close()
	log.Println("init success")

	//simple put and get tests
	db.Put([]byte("a"), []byte("content1"), nil)
	db.Put([]byte("key3"), []byte("content1"), nil)
	db.Put([]byte("key1"), []byte("content2"), nil)
	db.Put([]byte("key2"), []byte("content2"), nil)

	result1, _ := db.Get([]byte("key1"), nil)
	log.Println("val of key1:", string(result1))

	result2, _ := db.Get([]byte("key2"), nil)
	log.Println("val of key2:", string(result2))

	//iterator with key prefix

	iter := db.NewIterator(nil, nil)
	iter.Seek([]byte("key"))

	key := iter.Key()
	value := iter.Value()

	fmt.Println(string(key), string(value))

	iter.Next()
	key2 := iter.Key()
	value2 := iter.Value()

	fmt.Println(string(key2), string(value2))

	iter.Next()
	key3 := iter.Key()
	value3 := iter.Value()

	fmt.Println(string(key3), string(value3))

	///batch write
	batch := new(leveldb.Batch)
	batch.Put([]byte("a1"), []byte("c1"))
	batch.Put([]byte("a2"), []byte("c2"))
	batch.Put([]byte("a3"), []byte("c3"))
	batch.Delete([]byte("a2"))
	db.Write(batch, nil)

	a1_result, _ := db.Get([]byte("a1"), nil)
	fmt.Println("batch write , read a1:", string(a1_result))

	a2_result, _ := db.Get([]byte("a2"), nil)
	fmt.Println("batch write , read a2:", string(a2_result))

	a3_result, _ := db.Get([]byte("a3"), nil)
	fmt.Println("batch write , read a3:", string(a3_result))

	db.OpenTransaction()

}

/// set //

func TestInterface(t *testing.T) {

}
