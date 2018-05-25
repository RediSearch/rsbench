package main

import (
	"flag"
	"os"
	"time"

	"github.com/RedisLabs/redisearch-go/redisearch"
	"github.com/RedisLabs/rsbench/indexer"
	"github.com/RedisLabs/rsbench/parser"
)

func main() {

	reader := flag.String("reader", "", "Reader to use (if set) [wiki_abs|wiki_full|reddit|twitter]")
	path := flag.String("path", "./", "folder/file path")
	cons := flag.Int("conns", 100, "Concurrent connections to redis")
	files := flag.Int("rnum", 10, "Number of concurrent file readers")
	hosts := flag.String("hosts", "localhost:6379", "Redis host(s), comma separated list of ip:port pairs. Use a single value for non cluster version")
	index := flag.String("index", "idx", "Index name")
	query := flag.String("query", "", "Query to benchmark (if set)")
	duration := flag.Int("duration", 5, "Duration to run the query benchmark for")
	csv := flag.Bool("csv", false, "If set, we dump the output report as CSV")
	chunk := flag.Int("chunk", 1, "Indexing chunk size")

	flag.Parse()
	if *reader == "" && *query == "" {
		panic("Must have query or reader!")
	}

	if *reader != "" {
		var sp indexer.SchemaProvider
		var rd indexer.DocumentParser

		switch *reader {
		case "wiki_abs":
			rd = indexer.NewFolderReader(*path, "*.xml", *files, indexer.DocumentReaderOpenerFunc(parser.WikiAbstractReaderOpen))
			sp = indexer.SchemaProviderFunc(parser.WikipediaSchema)
		case "wiki_full":
			rd = indexer.NewFolderReader(*path, "*.bz2", *files, indexer.DocumentReaderOpenerFunc(parser.WikiArticleReaderOpen))
			sp = indexer.SchemaProviderFunc(parser.WikipediaSchema)
		case "reddit":
			rd = indexer.NewFolderReader(*path, "*.bz2", *files, indexer.DocumentReaderOpenerFunc(parser.RedditReaderOpen))
			sp = indexer.SchemaProviderFunc(parser.RedditSchema)
		case "twitter":
			rd = indexer.NewFolderReader(*path, "*.bz2", *files, indexer.DocumentReaderOpenerFunc(parser.TwitterReaderOpen))
			sp = indexer.SchemaProviderFunc(parser.TwitterSchema)
		case "stack":
			rd = indexer.NewSingleFileReader(*path, indexer.DocumentReaderOpenerFunc(parser.StackExchangeReaderOpen))
			sp = indexer.SchemaProviderFunc(parser.StackSchema)
		default:
			panic("Inavlid reader: " + *reader)
		}

		ch := make(chan redisearch.Document, *cons**chunk)

		if err := rd.Start(ch); err != nil {
			panic(err)
		}

		idx := indexer.New(*index, *hosts, *cons, ch, nil, sp, *chunk)
		idx.Start()
		if idx.GetNumIndexed() == 0 {
			panic("No documents indexed!")
		}
	}
	if *query != "" {

		client := redisearch.NewClient(*hosts, *index)

		b := NewQueryBenchmark(client, *query, *cons, time.Second*time.Duration(*duration))
		//fmt.Printf("Starting benchmark for %v\n", time.Until(b.endTime))
		b.Run()
		if *csv {
			b.DumpCSV(os.Stdout)
		} else {
			b.DumpJson(os.Stdout)
		}

	}
}
