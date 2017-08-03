package main

import (
	"flag"

	"github.com/RedisLabs/redisearch-go/redisearch"
	"github.com/RedisLabs/rsbench/indexer"
	"github.com/RedisLabs/rsbench/parser"
)

func main() {

	reader := flag.String("reader", "wiki", "Reader to use (wiki|wiki-article|reddit)")
	path := flag.String("path", "./", "folder/file path")
	cons := flag.Int("conns", 100, "Concuurent connections to redis")
	files := flag.Int("rnum", 10, "Number of concurrent file readers")
	host := flag.String("host", "localhost:6379", "Redis host")
	index := flag.String("index", "idx", "Index name")
	chunk := flag.Int("chunk", 10, "How many documents to index at a time")
	ndocs := flag.Uint64("limit", 0, "Exit after indexing this many documents (0 is unlimited)")

	flag.Parse()
	var sp indexer.SchemaProvider
	var rd indexer.DocumentParser

	switch *reader {
	case "wiki-abstract":
		rd = indexer.NewFolderReader(*path, "*.xml", *files, indexer.DocumentReaderOpenerFunc(parser.WikiAbstractReaderOpen))
		sp = indexer.SchemaProviderFunc(parser.WikipediaSchema)
	case "wiki-article":
		rd = indexer.NewFolderReader(*path, "*.xml", *files, indexer.DocumentReaderOpenerFunc(parser.WikiArticleReaderOpen))
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

	idx := indexer.New(*index, *host, ch, nil, sp, indexer.IndexerOptions{
		Concurrency: *cons, ChunkSize: *chunk, Limit: *ndocs})
	idx.Start()
}
