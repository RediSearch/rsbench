package indexer

import (
	"io"
	"log"
	"math"
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

type IndexerOptions struct {
	Concurrency int
	ChunkSize   int
	Limit       uint64
}

type Indexer struct {
	IndexerOptions
	client    *redisearch.Client
	chunkSize int
	ch        chan redisearch.Document
	parser    DocumentParser
	sp        SchemaProvider
	wg        sync.WaitGroup
	counter   uint64
	bytes     uint64
	lastBytes uint64
	lastCount uint64
	lastTime  time.Time
}

func (idx *Indexer) loop() {

	st := time.Now()
	interval := uint64(math.Max(float64(idx.ChunkSize), 50))

	// Buffer Size
	buf := make([]redisearch.Document, 0)
	bytes := uint64(0)

	flush := func() {
		mb := uint64(0)
		for _, doc := range buf {
			for key, val := range doc.Properties {
				val, isString := val.(string)
				mb += uint64(len(key))
				if isString {
					mb += uint64(len(val))
				}
			}
		}

		if err := idx.client.IndexOptions(redisearch.IndexingOptions{NoSave: true}, buf...); err != nil {
			log.Printf("Error indexing %s\n", err)
		}
		bytes = atomic.AddUint64(&idx.bytes, mb)
		buf = make([]redisearch.Document, 0)
	}

	for doc := range idx.ch {
		if len(buf) == idx.ChunkSize {
			flush()
		}

		buf = append(buf, doc)

		x := atomic.AddUint64(&idx.counter, 1)
		if idx.Limit > 0 && x >= idx.Limit {
			idx.wg.Done()
			return
		}

		if x%interval == 0 {
			elapsed := time.Since(st)
			currentTime := time.Since(idx.lastTime)
			currentMB := float64(bytes-idx.lastBytes) / math.Pow(1024, 2)
			log.Printf("Indexed %d docs in %0.3fs, rate %.02f Docs/sec, %.02fMB/s", x,
				elapsed.Seconds(),
				float64(x-idx.lastCount)/currentTime.Seconds(),
				float64(currentMB/currentTime.Seconds()))
			idx.lastCount = x
			idx.lastTime = time.Now()
			idx.lastBytes = bytes
		}
	}

	flush()
	idx.wg.Done()
}

func New(name, host string, ch chan redisearch.Document, parser DocumentParser, sp SchemaProvider, options IndexerOptions) *Indexer {
	return &Indexer{
		IndexerOptions: options,
		client:         redisearch.NewClient(host, name),
		ch:             ch,
		parser:         parser,
		sp:             sp,
		wg:             sync.WaitGroup{},
		counter:        0,
		lastCount:      0,
		lastTime:       time.Now(),
	}
}

// SetupIndex - Drops the existing index and recreates it
func (idx *Indexer) SetupIndex() {
	log.Print("Dropping old index")
	idx.client.Drop()

	log.Print("Creating new index")
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
}

// Validate - Validates that the index actually exists
func (idx *Indexer) Validate() error {
	_, err := idx.client.Info()
	return err
}

// LoadDocuments - Loads the documents into the index. Returns once complete
func (idx *Indexer) LoadDocuments() {
	for i := 0; i < idx.Concurrency; i++ {
		idx.wg.Add(1)
		go idx.loop()
	}
	idx.wg.Wait()
}
