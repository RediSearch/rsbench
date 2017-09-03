package indexer

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
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

type SchemaProvider interface {
	Schema() *redisearch.Schema
}

type SchemaProviderFunc func() *redisearch.Schema

func (s SchemaProviderFunc) Schema() *redisearch.Schema {
	return s()
}

type DocumentReaderOpener interface {
	Open(io.Reader) (DocumentReader, error)
}

type DocumentReaderOpenerFunc func(io.Reader) (DocumentReader, error)

func (f DocumentReaderOpenerFunc) Open(r io.Reader) (DocumentReader, error) {
	return f(r)
}

type Indexer struct {
	client       *redisearch.Client
	concurrency  int
	ch           chan redisearch.Document
	parser       DocumentParser
	sp           SchemaProvider
	wg           sync.WaitGroup
	counter      uint64
	lastCount    uint64
	totalLatency uint64
	lastTime     time.Time
}

func (idx *Indexer) loop() {

	cw := csv.NewWriter(os.Stdout)
	st := time.Now()
	N := 10
	chunk := make([]redisearch.Document, N)
	dx := 0
	for doc := range idx.ch {
		chunk[dx] = doc
		dx++
		if dx == N {
			dx = 0

			t1 := time.Now()
			if err := idx.client.IndexOptions(redisearch.IndexingOptions{NoSave: true}, chunk...); err != nil {
				//log.Printf("Error indexing %s: %s\n", doc.Id, err)
				//continue
			}
			latency := time.Since(t1)
			atomic.AddUint64(&idx.totalLatency, uint64(latency))
			if x := atomic.AddUint64(&idx.counter, uint64(N)); x%10000 == 0 && time.Since(idx.lastTime) > time.Second {
				elapsed := time.Since(st)
				currentTime := time.Since(idx.lastTime)
				avgLatency := time.Duration(idx.totalLatency/idx.counter).Seconds() * 1000
				cw.Write([]string{
					strconv.FormatUint(x, 10),
					strconv.FormatFloat(elapsed.Seconds(), 'f', 2, 32),
					strconv.FormatFloat(float64(x-idx.lastCount)/currentTime.Seconds(), 'f', 2, 32),
					strconv.FormatFloat(avgLatency, 'f', 2, 32),
				})
				cw.Flush()
				//log.Printf("Indexed %d docs in %v, rate %.02fdocs/sec", x, elapsed, float64(x-idx.lastCount)/currentTime.Seconds())
				idx.lastCount = x
				idx.lastTime = time.Now()
			}

		}
	}
	idx.wg.Done()
}

func New(name, host string, concurrency int, ch chan redisearch.Document, parser DocumentParser, sp SchemaProvider) *Indexer {
	return &Indexer{
		client:      redisearch.NewClient(host, name),
		concurrency: concurrency,
		ch:          ch,
		parser:      parser,
		sp:          sp,
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
