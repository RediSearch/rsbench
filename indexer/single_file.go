package indexer

import (
	"os"

	"log"

	"io"

	"github.com/RedisLabs/redisearch-go/redisearch"
)

type SingleFileReader struct {
	name   string
	opener DocumentReaderOpener
	stopch chan bool
}

func NewSingleFileReader(name string, opener DocumentReaderOpener) DocumentParser {
	return &SingleFileReader{
		name:   name,
		opener: opener,
		stopch: make(chan bool),
	}
}

func (r *SingleFileReader) Start(ch chan<- redisearch.Document) error {
	fp, err := os.Open(r.name)
	if err != nil {
		return err
	}
	dr, err := r.opener.Open(fp)
	if err != nil {
		return err
	}

	go func() {
		var err error
		var doc redisearch.Document
		for err == nil {
			if doc, err = dr.Read(); err == nil {
				select {
				// stop if we're signaled by stopch
				case <-r.stopch:
					err = io.EOF
				// read the next document
				case ch <- doc:
				}

			}
		}
		log.Println("Single file reader exiting, error:", err)
	}()

	return nil
}

func (r *SingleFileReader) Stop() {
	r.stopch <- true
	close(r.stopch)
}
