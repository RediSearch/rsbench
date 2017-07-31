package main

import (
	"github.com/RedisLabs/redisearch-go/redisearch"
	"github.com/RedisLabs/rsbench/indexer"
	"github.com/RedisLabs/rsbench/parser"
)

func main() {

	// sfs := []indexer.DocumentParser{
	// 	indexer.NewSingleFileReader("/Users/dvirvolk/Downloads/reddit_data/2016/RC_2016-08.bz2",
	// 		indexer.DocumentReaderOpenerFunc(parser.RedditReaderOpen)),
	// 	indexer.NewSingleFileReader("/Users/dvirvolk/Downloads/reddit_data/2008/RC_2008-01.bz2",
	// 		indexer.DocumentReaderOpenerFunc(parser.RedditReaderOpen)),
	// }
	sfs := []indexer.DocumentParser{
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract1.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract2.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract3.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract4.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract5.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract6.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract7.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract8.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract9.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
		indexer.NewSingleFileReader("/Users/dvirvolk/data/wiki/enwiki-latest-abstract10.xml",
			indexer.DocumentReaderOpenerFunc(parser.WikiReaderOpen)),
	}
	ch := make(chan redisearch.Document, 1000)

	for _, sf := range sfs {

		if err := sf.Start(ch); err != nil {
			panic(err)
		}
	}

	idx := indexer.New("wik", "localhost:6379", 100, ch, nil)
	idx.Start()
}
