package indexer

import (
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RedisLabs/redisearch-go/redisearch"
)

type DocumentParser interface {
	Start(chan<- redisearch.Document) error
	Stop()
}

type DocumentReader interface {
	Read() (redisearch.Document, error)
}

type DocumentReaderOpener interface {
	Open(io.Reader) (DocumentReader, error)
}

type DocumentReaderOpenerFunc func(io.Reader) (DocumentReader, error)

func (f DocumentReaderOpenerFunc) Open(r io.Reader) (DocumentReader, error) {
	return f(r)
}

type Indexer struct {
	client      *redisearch.Client
	concurrency int
	ch          chan redisearch.Document
	parser      DocumentParser
	wg          sync.WaitGroup
	counter     uint64
	lastCount   uint64
	lastTime    time.Time
}

func (idx *Indexer) loop() {
	docs := make([]redisearch.Document, 1)
	st := time.Now()
	for docs[0] = range idx.ch {

		//idx.client.Index(docs, redisearch.IndexingOptions{NoSave: true})
		if x := atomic.AddUint64(&idx.counter, 1); x%10000 == 0 {
			elapsed := time.Since(st)
			currentTime := time.Since(idx.lastTime)
			log.Printf("Indexed %d docs in %v, rate %.02fdocs/sec", x, elapsed, float64(x-idx.lastCount)/currentTime.Seconds())
			idx.lastCount = x
			idx.lastTime = time.Now()
		}
	}
	idx.wg.Done()
}

func New(name, host string, concurrency int, ch chan redisearch.Document, parser DocumentParser) *Indexer {
	return &Indexer{
		client:      redisearch.NewClient(host, name),
		concurrency: concurrency,
		ch:          ch,
		parser:      parser,
		wg:          sync.WaitGroup{},
		counter:     0,
		lastCount:   0,
		lastTime:    time.Now(),
	}
}

func (idx *Indexer) Start() {
	idx.client.Drop()
	// sc := redisearch.NewSchema(redisearch.DefaultOptions).
	// 	AddField(redisearch.NewTextField("body")).
	// 	AddField(redisearch.NewTextField("author")).
	// 	AddField(redisearch.NewTextField("sub")).
	// 	AddField(redisearch.NewNumericField("date"))
	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("body")).
		AddField(redisearch.NewTextField("title")).
		AddField(redisearch.NewTextField("url"))

	if err := idx.client.CreateIndex(sc); err != nil {
		panic(err)
	}
	for i := 0; i < idx.concurrency; i++ {
		idx.wg.Add(1)
		go idx.loop()
	}
	idx.wg.Wait()
}
