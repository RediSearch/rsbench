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

	flag.Parse()
	var sp indexer.SchemaProvider
	var rd indexer.DocumentParser

	switch *reader {
	case "wiki":
		rd = indexer.NewFolderReader(*path, "*.xml", 8, indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen))
		sp = indexer.SchemaProviderFunc(parser.WikipediaSchema)
	case "reddit":
		rd = indexer.NewFolderReader(*path, "*.bz2", 8, indexer.DocumentReaderOpenerFunc(parser.RedditReaderOpen))
		sp = indexer.SchemaProviderFunc(parser.RedditSchema)
	default:
		panic("Inavlid reader: " + *reader)
	}

	ch := make(chan redisearch.Document, 100)

	if err := rd.Start(ch); err != nil {
		panic(err)
	}

	idx := indexer.New("idx", "localhost:6379", 100, ch, nil, sp)
	idx.Start()
}
