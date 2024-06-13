module github.com/xlander-io/kv_leveldb

go 1.22.1

require (
	github.com/syndtr/goleveldb v1.0.0
	github.com/xlander-io/kv v0.0.0-20240612131354-faf15072c970
)

require github.com/golang/snappy v0.0.4 // indirect

// replace github.com/xlander-io/kv => ../kv.git
