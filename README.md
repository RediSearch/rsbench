[![license](https://img.shields.io/github/license/RediSearch/rsbench.svg)](https://github.com/RediSearch/rsbench)
[![GoDoc](https://godoc.org/github.com/RediSearch/rsbench?status.svg)](https://godoc.org/github.com/RediSearch/rsbench)
[![Forum](https://img.shields.io/badge/Forum-RediSearch-blue)](https://forum.redislabs.com/c/modules/redisearch/)

# rsbench
Benchmarks for the RediSearch module

## Usage

```
Usage of ./rsbench:
  -chunk int
    	Indexing chunk size (default 1)
  -conns int
    	Concurrent connections to redis (default 100)
  -csv
    	If set, we dump the output report as CSV
  -duration int
    	Duration to run the query benchmark for (default 5)
  -hosts string
    	Redis host(s), comma separated list of ip:port pairs. Use a single value for non cluster version (default "localhost:6379")
  -index string
    	Index name (default "idx")
  -path string
    	folder/file path (default "./")
  -query string
    	Query to benchmark (if set)
  -reader string
    	Reader to use (if set) [wiki_abs|wiki_full|reddit|twitter]
  -rnum int
    	Number of concurrent file readers (default 10)
```
