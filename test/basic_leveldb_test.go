package test

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

func TestBasic(t *testing.T) {

	db := prepareTestLevelDB()
	defer db.Close()

	//simple put and get tests
	db.Put([]byte("a"), []byte("contenta"), nil)
	db.Put([]byte("key3"), []byte("content3"), nil)
	db.Put([]byte("key1"), []byte("content1"), nil)
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

	// db.OpenTransaction()
}

/// set //

func TestInterface(t *testing.T) {

}

func TestSnapshot(t *testing.T) {
	db := prepareTestLevelDB()
	defer db.Close()
	prepareDefaultData(db)

	// 首次snapshot
	snap1, _ := db.GetSnapshot()
	fmt.Println(snap1.String())

	// 测试snapshot的基本功能
	{
		key1_result, _ := snap1.Get([]byte("key1"), nil)
		key2_result, _ := snap1.Get([]byte("key2"), nil)
		key3_result, _ := snap1.Get([]byte("key3"), nil)

		if v := []byte("content1"); !bytes.Equal(key1_result, v) {
			t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key1_result))
		}
		if v := []byte("content2"); !bytes.Equal(key2_result, v) {
			t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key2_result))
		}
		if v := []byte("content3"); !bytes.Equal(key3_result, v) {
			t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key3_result))
		}
	}

	// 继续向数据库里面更新数据，并测试db的Get数据是否最新
	{
		db.Put([]byte("key2"), []byte("new content2"), nil)
		db.Put([]byte("key1"), []byte("new content1"), nil)
		db.Put([]byte("key3"), []byte("new content3"), nil)

		key1_result, _ := db.Get([]byte("key1"), nil)
		key2_result, _ := db.Get([]byte("key2"), nil)
		key3_result, _ := db.Get([]byte("key3"), nil)

		if v := []byte("new content1"); !bytes.Equal(key1_result, v) {
			t.Errorf("key1_result new: expect [%s], but get [%s]", v, string(key1_result))
		}
		if v := []byte("new content2"); !bytes.Equal(key2_result, v) {
			t.Errorf("key1_result new: expect [%s], but get [%s]", v, string(key2_result))
		}
		if v := []byte("new content3"); !bytes.Equal(key3_result, v) {
			t.Errorf("key1_result new: expect [%s], but get [%s]", v, string(key3_result))
		}
	}

	// 在数据库被更新之后，再次测试之前snapshot的数据是否改变
	{
		key1_result, _ := snap1.Get([]byte("key1"), nil)
		key2_result, _ := snap1.Get([]byte("key2"), nil)
		key3_result, _ := snap1.Get([]byte("key3"), nil)

		if v := []byte("content1"); !bytes.Equal(key1_result, v) {
			t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key1_result))
		}
		if v := []byte("content2"); !bytes.Equal(key2_result, v) {
			t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key2_result))
		}
		if v := []byte("content3"); !bytes.Equal(key3_result, v) {
			t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key3_result))
		}
	}

	// 进行第二次的snapshot测试
	{
		snap2, _ := db.GetSnapshot()
		fmt.Println(snap2.String())

		// 测试第二次的snapshot的数据是否正常
		{
			key1_result, _ := snap2.Get([]byte("key1"), nil)
			key2_result, _ := snap2.Get([]byte("key2"), nil)
			key3_result, _ := snap2.Get([]byte("key3"), nil)

			if v := []byte("new content1"); !bytes.Equal(key1_result, v) {
				t.Errorf("key1_result snap2: expect [%s], but get [%s]", v, string(key1_result))
			}
			if v := []byte("new content2"); !bytes.Equal(key2_result, v) {
				t.Errorf("key1_result snap2: expect [%s], but get [%s]", v, string(key2_result))
			}
			if v := []byte("new content3"); !bytes.Equal(key3_result, v) {
				t.Errorf("key1_result snap2: expect [%s], but get [%s]", v, string(key3_result))
			}
		}
		// 测试在第二次snapshot之后，第一次的snapshot是否正常
		{
			key1_result, _ := snap1.Get([]byte("key1"), nil)
			key2_result, _ := snap1.Get([]byte("key2"), nil)
			key3_result, _ := snap1.Get([]byte("key3"), nil)

			if v := []byte("content1"); !bytes.Equal(key1_result, v) {
				t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key1_result))
			}
			if v := []byte("content2"); !bytes.Equal(key2_result, v) {
				t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key2_result))
			}
			if v := []byte("content3"); !bytes.Equal(key3_result, v) {
				t.Errorf("key1_result snap1: expect [%s], but get [%s]", v, string(key3_result))
			}
		}
		// 测试在两次snapshot之后，直接访问db的数据是否访问的最新的数据
		{
			key1_result, _ := db.Get([]byte("key1"), nil)
			key2_result, _ := db.Get([]byte("key2"), nil)
			key3_result, _ := db.Get([]byte("key3"), nil)

			if v := []byte("new content1"); !bytes.Equal(key1_result, v) {
				t.Errorf("key1_result new: expect [%s], but get [%s]", v, string(key1_result))
			}
			if v := []byte("new content2"); !bytes.Equal(key2_result, v) {
				t.Errorf("key1_result new: expect [%s], but get [%s]", v, string(key2_result))
			}
			if v := []byte("new content3"); !bytes.Equal(key3_result, v) {
				t.Errorf("key1_result new: expect [%s], but get [%s]", v, string(key3_result))
			}
		}
	}
}

func prepareTestLevelDB() *leveldb.DB {
	file_path := "./db"

	err := os.RemoveAll(file_path) // 必须重置
	if err != nil {
		log.Fatal("RemoveAll err:", err)
	}

	db, err := leveldb.OpenFile(file_path, nil)
	if err != nil {
		log.Fatal("init err:", err)
	}
	log.Println("init success")
	return db
}

func prepareDefaultData(db *leveldb.DB) {
	db.Put([]byte("a"), []byte("contenta"), nil)
	db.Put([]byte("key3"), []byte("content3"), nil)
	db.Put([]byte("key1"), []byte("content1"), nil)
	db.Put([]byte("key2"), []byte("content2"), nil)

	batch := new(leveldb.Batch)
	batch.Put([]byte("a1"), []byte("c1"))
	batch.Put([]byte("a2"), []byte("c2"))
	batch.Put([]byte("a3"), []byte("c3"))
	batch.Delete([]byte("a2"))
	db.Write(batch, nil)
}
