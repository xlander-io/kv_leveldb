package test

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb"
)

func NewDB() *leveldb.DB {
	db_path := "./leveldb_test.db"
	os.RemoveAll(db_path)
	db, err := leveldb.OpenFile(db_path, nil)
	if err != nil {
		log.Fatal("init err:", err)
	}
	log.Println("init db success")
	return db
}

func TestSimple(t *testing.T) {
	db := NewDB()
	//simple put and get tests
	db.Put([]byte("a"), []byte("content1"), nil)
	db.Put([]byte("key3"), []byte("content1"), nil)
	db.Put([]byte("key1"), []byte("content2"), nil)
	db.Put([]byte("key2"), []byte("content2"), nil)

	result1, _ := db.Get([]byte("key1"), nil)
	log.Println("val of key1:", string(result1))

	result2, _ := db.Get([]byte("key2"), nil)
	log.Println("val of key2:", string(result2))

}

func TestBatch(t *testing.T) {

	db := NewDB()
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

}

func TestIterator(t *testing.T) {

	db := NewDB()
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
}

func TestSnapshot(t *testing.T) {
	db := NewDB()

	initial := map[string]string{
		"k1": "v1", "k2": "v2", "k3": "", "k4": "",
	}
	for k, v := range initial {
		db.Put([]byte(k), []byte(v), nil)
	}
	snapshot, err := db.GetSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range initial {
		got, err := snapshot.Get([]byte(k), nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Fatalf("Unexpected value: %s, expect: %s", got, v)
		}
	}
	/////// init k-v finished/////

	// Flush more modifications into the database, ensure the snapshot
	// isn't affected.
	var (
		update = map[string]string{"k1": "v1-b", "k3": "v3-b"}
		insert = map[string]string{"k5": "v5-b"}
		delete = map[string]string{"k2": ""}
	)
	for k, v := range update {
		db.Put([]byte(k), []byte(v), nil)
	}
	for k, v := range insert {
		db.Put([]byte(k), []byte(v), nil)
	}
	for k := range delete {
		db.Delete([]byte(k), nil)
	}

	for k, v := range initial {
		got, err := snapshot.Get([]byte(k), nil)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, []byte(v)) {
			t.Fatalf("Unexpected value: %s, expect: %s", got, v)
		}
	}

	for k := range insert {
		got, err := snapshot.Get([]byte(k), nil)
		if err == nil || len(got) != 0 {
			t.Fatal("Unexpected value")
		}
	}
	for k := range delete {
		got, err := snapshot.Get([]byte(k), nil)
		if err != nil || len(got) == 0 {
			t.Fatal("Unexpected deletion")
		}
	}

}

func Benchmark_ShortKey(b *testing.B) {
	db := NewDB()

	var KEY = []byte("short")
	var CONTENT = []byte("content")

	log.Println("len(KEY): ", len(KEY))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put(KEY, CONTENT, nil)
		db.Get(KEY, nil)
	}
}

func Benchmark_LongKey(b *testing.B) {
	db := NewDB()

	// https://www.lipsum.com
	KEY := []byte("Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.")
	KEY = append(KEY, []byte("Contrary to popular belief, Lorem Ipsum is not simply random text. It has roots in a piece of classical Latin literature from 45 BC, making it over 2000 years old. Richard McClintock, a Latin professor at Hampden-Sydney College in Virginia, looked up one of the more obscure Latin words, consectetur, from a Lorem Ipsum passage, and going through the cites of the word in classical literature, discovered the undoubtable source. Lorem Ipsum comes from sections 1.10.32 and 1.10.33 of \"de Finibus Bonorum et Malorum\" (The Extremes of Good and Evil) by Cicero, written in 45 BC. This book is a treatise on the theory of ethics, very popular during the Renaissance. The first line of Lorem Ipsum, \"Lorem ipsum dolor sit amet..\", comes from a line in section 1.10.32.")...)
	KEY = append(KEY, []byte("The standard chunk of Lorem Ipsum used since the 1500s is reproduced below for those interested. Sections 1.10.32 and 1.10.33 from \"de Finibus Bonorum et Malorum\" by Cicero are also reproduced in their exact original form, accompanied by English versions from the 1914 translation by H. Rackham.")...)
	var CONTENT = []byte("content")
	log.Println("len(KEY): ", len(KEY))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Put(KEY, CONTENT, nil)
		db.Get(KEY, nil)
	}
}
