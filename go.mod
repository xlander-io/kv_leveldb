module github.com/xlander-io/kv_leveldb

go 1.22.1

require (
	github.com/syndtr/goleveldb v1.0.0
	github.com/xlander-io/kv_interface v0.0.0-00010101000000-000000000000
)

require github.com/golang/snappy v0.0.4 // indirect

replace github.com/xlander-io/kv_interface => ../kv_interface.git
