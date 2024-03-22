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

	{
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

	{
		snap, _ := db.GetSnapshot()
		fmt.Println(snap.String())

		{
			key1_result, _ := snap.Get([]byte("key1"), nil)
			key2_result, _ := snap.Get([]byte("key2"), nil)
			key3_result, _ := snap.Get([]byte("key3"), nil)

			fmt.Println("key1_result snap: ", string(key1_result))
			fmt.Println("key2_result snap: ", string(key2_result))
			fmt.Println("key3_result snap: ", string(key3_result))
		}

		{
			db.Put([]byte("key2"), []byte("new content2"), nil)
			db.Put([]byte("key1"), []byte("new content1"), nil)
			db.Put([]byte("key3"), []byte("new content3"), nil)

			key1_result, _ := db.Get([]byte("key1"), nil)
			key2_result, _ := db.Get([]byte("key2"), nil)
			key3_result, _ := db.Get([]byte("key3"), nil)

			fmt.Println("key1_result new: ", string(key1_result))
			fmt.Println("key2_result new: ", string(key2_result))
			fmt.Println("key3_result new: ", string(key3_result))
		}

		{
			key1_result, _ := snap.Get([]byte("key1"), nil)
			key2_result, _ := snap.Get([]byte("key2"), nil)
			key3_result, _ := snap.Get([]byte("key3"), nil)

			fmt.Println("key1_result snap: ", string(key1_result))
			fmt.Println("key2_result snap: ", string(key2_result))
			fmt.Println("key3_result snap: ", string(key3_result))
		}

		{
			snap2, _ := db.GetSnapshot()
			fmt.Println(snap2.String())

			{
				key1_result, _ := snap2.Get([]byte("key1"), nil)
				key2_result, _ := snap2.Get([]byte("key2"), nil)
				key3_result, _ := snap2.Get([]byte("key3"), nil)

				fmt.Println("key1_result snap2: ", string(key1_result))
				fmt.Println("key2_result snap2: ", string(key2_result))
				fmt.Println("key3_result snap2: ", string(key3_result))
			}
			{
				key1_result, _ := snap.Get([]byte("key1"), nil)
				key2_result, _ := snap.Get([]byte("key2"), nil)
				key3_result, _ := snap.Get([]byte("key3"), nil)

				fmt.Println("key1_result snap1: ", string(key1_result))
				fmt.Println("key2_result snap1: ", string(key2_result))
				fmt.Println("key3_result snap1: ", string(key3_result))
			}
			{
				key1_result, _ := db.Get([]byte("key1"), nil)
				key2_result, _ := db.Get([]byte("key2"), nil)
				key3_result, _ := db.Get([]byte("key3"), nil)

				fmt.Println("key1_result last: ", string(key1_result))
				fmt.Println("key2_result last: ", string(key2_result))
				fmt.Println("key3_result last: ", string(key3_result))
			}
		}
	}
}

/// set //

func TestInterface(t *testing.T) {

}
