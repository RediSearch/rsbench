package main

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/RedisLabs/redisearch-go/redisearch"
)

type QueryBenchmark struct {
	query        *redisearch.Query
	client       *redisearch.Client
	concurrency  int
	endTime      time.Time
	startTime    time.Time
	runDuration  time.Duration
	numRequests  int
	totalLatency time.Duration
	wg           sync.WaitGroup
	reportch     chan time.Duration
}

func NewQueryBenchmark(c *redisearch.Client, q string, concurrency int, runTime time.Duration) *QueryBenchmark {
	return &QueryBenchmark{
		query:       redisearch.NewQuery(q).SetFlags(redisearch.QueryNoContent|redisearch.QueryVerbatim).Limit(0, 1),
		client:      c,
		concurrency: concurrency,
		endTime:     time.Now().Add(runTime),
		reportch:    make(chan time.Duration, concurrency),
		numRequests: 0,
	}
}

func (b *QueryBenchmark) DumpCSV(out io.Writer) error {
	cw := csv.NewWriter(out)
	vals := []string{
		b.query.Raw,
		strconv.FormatInt(int64(b.concurrency), 10),
		strconv.FormatFloat(b.RequestsPerSecond(), 'f', 2, 64),
		strconv.FormatFloat(b.AverageLatency(), 'f', 2, 64),
	}
	if e := cw.Write(vals); e != nil {
		return e
	}
	cw.Flush()
	return nil
}
func (b *QueryBenchmark) DumpJson(out io.Writer) error {
	values := map[string]interface{}{
		"query":       b.query.Raw,
		"concurrency": b.concurrency,
		"rps":         b.RequestsPerSecond(),
		"latency":     b.AverageLatency(),
	}

	enc := json.NewEncoder(out)
	enc.SetIndent("", "\t")
	return enc.Encode(values)
}

func (b *QueryBenchmark) loop() {
	tm := time.Now()
	for tm.Before(b.endTime) {
		_, _, err := b.client.Search(b.query)
		if err == nil {
			b.reportch <- time.Since(tm)
		}
		tm = time.Now()

	}
	b.wg.Done()

}

func (b *QueryBenchmark) RequestsPerSecond() float64 {
	return float64(b.numRequests) / time.Since(b.startTime).Seconds()
}

func (b *QueryBenchmark) AverageLatency() float64 {
	return time.Duration(uint64(b.totalLatency)/uint64(b.numRequests)).Seconds() * 1000
}

func (b *QueryBenchmark) Run() error {
	for i := 0; i < b.concurrency; i++ {
		b.wg.Add(1)
		go b.loop()
	}
	go func() {
		b.wg.Wait()
		close(b.reportch)
	}()
	b.startTime = time.Now()
	lastSample := time.Now()
	for {
		if latency, ok := <-b.reportch; ok {
			b.numRequests++
			b.totalLatency += latency
			if time.Since(lastSample) > time.Second {
				// fmt.Printf("%d requests in %v, rate: %.02fr/s, Avg. latency: %.02fms\n", b.numRequests, time.Since(b.startTime),
				// 	b.RequestsPerSecond(),
				// 	b.AverageLatency())
				lastSample = time.Now()
			}
		} else {
			b.runDuration = time.Since(b.startTime)
			break
		}
	}

	// log.Printf("%d requests for %s in %v, rate: %.02fr/s, Avg. Latency %.02fms", b.numRequests, b.query.Raw, b.runDuration,
	// 	b.RequestsPerSecond(), b.AverageLatency())

	return nil
}
