package parser

import (
	"io"

	"fmt"

	"github.com/RedisLabs/redisearch-go/redisearch"
	"github.com/RedisLabs/rsbench/indexer"
	wp "github.com/dustin/go-wikiparse"
)

type wikipediaDumpReader struct {
	parser wp.Parser
}

func (wr *wikipediaDumpReader) Read() (doc redisearch.Document, err error) {
	page, err := wr.parser.Next()
	if err != nil {
		return
	}

	doc = redisearch.NewDocument(fmt.Sprintf("WP_%d", page.ID), 1.0).
		Set("title", page.Title).
		Set("body", page.Revisions[0].Text).
		Set("url", wp.URLForFile(page.Title))
	return
}

// WikiArticleReaderOpen Create new reader for wikipedia article dumps
func WikiArticleReaderOpen(r io.Reader) (wr indexer.DocumentReader, err error) {
	parser, err := wp.NewParser(r)
	if err != nil {
		return
	}
	return &wikipediaDumpReader{parser: parser}, nil
}
