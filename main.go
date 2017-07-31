package main

import (
	"flag"

	"github.com/RedisLabs/redisearch-go/redisearch"
	"github.com/RedisLabs/rsbench/indexer"
	"github.com/RedisLabs/rsbench/parser"
)

func main() {

	reader := flag.String("reader", "wiki", "Reader to use (wiki|reddit)")
	path := flag.String("path", "./", "folder/file path")
	cons := flag.Int("conns", 100, "Concuurent connections to redis")
	files := flag.Int("rnum", 10, "Number of concurrent file readers")
	host := flag.String("host", "localhost:6379", "Redis host")
	index := flag.String("index", "idx", "Index name")

	flag.Parse()
	var sp indexer.SchemaProvider
	var rd indexer.DocumentParser

	switch *reader {
	case "wiki":
		rd = indexer.NewFolderReader(*path, "*.xml", *files, indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen))
		sp = indexer.SchemaProviderFunc(parser.WikipediaSchema)
	case "reddit":
		rd = indexer.NewFolderReader(*path, "*.bz2", *files, indexer.DocumentReaderOpenerFunc(parser.RedditReaderOpen))
		sp = indexer.SchemaProviderFunc(parser.RedditSchema)
	default:
		panic("Inavlid reader: " + *reader)
	}

	ch := make(chan redisearch.Document, *cons)

	if err := rd.Start(ch); err != nil {
		panic(err)
	}

	idx := indexer.New(*index, *host, *cons, ch, nil, sp)
	idx.Start()
}
