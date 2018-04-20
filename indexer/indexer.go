package indexer

import (
	"encoding/csv"
	"io"
	"log"
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
	chunkSize    int
	ch           chan redisearch.Document
	parser       DocumentParser
	sp           SchemaProvider
	wg           sync.WaitGroup
	counter      uint64
	lastCount    uint64
	totalLatency uint64
	lastDataSize uint64
	lastTime     time.Time
	cw           *csv.Writer
}

func (idx *Indexer) loop() {

	st := time.Now()
	N := idx.chunkSize
	chunk := make([]redisearch.Document, N)
	dx := 0
	for doc := range idx.ch {
		if doc.Id == "" {
			continue
		}
		chunk[dx] = doc
		dx++
		if dx == N {
			dx = 0

			t1 := time.Now()
			if err := idx.client.IndexOptions(redisearch.IndexingOptions{NoSave: true}, chunk...); err != nil {
				log.Printf("Error indexing %#v %s: %s\n", chunk, doc.Id, err)
				continue
			}
			latency := time.Since(t1)
			var totalSz uint64
			for i := range chunk {
				totalSz += uint64(chunk[i].EstimateSize())
			}
			dataSize := atomic.AddUint64(&idx.lastDataSize, uint64(totalSz))
			totalLatency := atomic.AddUint64(&idx.totalLatency, uint64(latency))
			if x := atomic.AddUint64(&idx.counter, uint64(N)); x%1000 == 0 && time.Since(idx.lastTime) > 5*time.Second {
				elapsed := time.Since(st)
				currentTime := time.Since(idx.lastTime)
				avgLatency := time.Duration(totalLatency/idx.counter).Seconds() * 1000
				dataRate := (float64(dataSize) / currentTime.Seconds()) / (1024 * 1024)

				idx.cw.Write([]string{
					strconv.FormatFloat(elapsed.Seconds(), 'f', 2, 32),
					strconv.FormatUint(x, 10),
					strconv.FormatFloat(float64(x-idx.lastCount)/currentTime.Seconds(), 'f', 2, 32),
					strconv.FormatFloat(avgLatency, 'f', 2, 32),
					strconv.FormatFloat(dataRate, 'f', 2, 32),
				})
				idx.cw.Flush()
				log.Printf("Indexed %d docs in %v, rate %.02fdocs/sec, latency %.02fms, dataRate: %.02fMB/s", x, elapsed,
					float64(x-idx.lastCount)/currentTime.Seconds(),
					avgLatency, dataRate)

				atomic.StoreUint64(&idx.lastCount, x)
				atomic.StoreUint64(&idx.lastDataSize, 0)
				idx.lastTime = time.Now()

			}

		}
	}
	idx.wg.Done()
}

func New(name, host string, concurrency int, ch chan redisearch.Document,
	parser DocumentParser, sp SchemaProvider, chunkSize int) *Indexer {
	ret := &Indexer{
		client:      redisearch.NewClient(host, name),
		concurrency: concurrency,
		ch:          ch,
		parser:      parser,
		sp:          sp,
		wg:          sync.WaitGroup{},
		counter:     0,
		lastCount:   0,
		lastTime:    time.Now(),
		chunkSize:   chunkSize,
		cw:          csv.NewWriter(os.Stdout),
	}

	ret.cw.Write([]string{
		"Time Elapsed",
		"Documents Indexed",
		"Documents/Second",
		"Avg. Latency",
		"MBs/Second",
	})
	ret.cw.Flush()
	return ret
}

func (idx *Indexer) Start() {
	idx.client.Drop()
	// sc := redisearch.NewSchema(redisearch.DefaultOptions).
	// 	AddField(redisearch.NewTextField("body")).
	// 	AddField(redisearch.NewTextField("author")).
	// 	AddField(redisearch.NewTextField("sub")).
	// 	AddField(redisearch.NewNumericField("date"))
	sc := idx.sp.Schema()

	if err := idx.client.CreateIndex(sc); err != nil {
		panic(err)
	}
	for i := 0; i < idx.concurrency; i++ {
		idx.wg.Add(1)
		go idx.loop()
	}
	idx.wg.Wait()
}
