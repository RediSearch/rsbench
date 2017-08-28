package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/RedisLabs/redisearch-go/redisearch"
)

type QueryBenchmark struct {
	query       *redisearch.Query
	client      *redisearch.Client
	concurrency int
	endTime     time.Time
	startTime   time.Time
	runDuration time.Duration
	numRequests int

	wg       sync.WaitGroup
	reportch chan error
}

func NewQueryBenchmark(c *redisearch.Client, q string, concurrency int, runTime time.Duration) *QueryBenchmark {
	return &QueryBenchmark{
		query:       redisearch.NewQuery(q).SetFlags(redisearch.QueryNoContent|redisearch.QueryVerbatim).Limit(0, 1),
		client:      c,
		concurrency: concurrency,
		endTime:     time.Now().Add(runTime),
		reportch:    make(chan error, concurrency),
		numRequests: 0,
	}
}

func (b *QueryBenchmark) loop() {

	for time.Now().Before(b.endTime) {
		_, _, err := b.client.Search(b.query)
		b.reportch <- err
	}
	b.wg.Done()

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
		if _, ok := <-b.reportch; ok {
			b.numRequests++
			if time.Since(lastSample) > time.Second {
				fmt.Printf("%d requests in %v, rate: %.02fr/s\n", b.numRequests, time.Since(b.startTime),
					float64(b.numRequests)/time.Since(b.startTime).Seconds())
				lastSample = time.Now()
			}
		} else {
			b.runDuration = time.Since(b.startTime)
			break
		}
	}

	fmt.Printf("%d requests for %s in %v, rate: %.02fr/s\n", b.numRequests, b.query.Raw, b.runDuration,
		float64(b.numRequests)/b.runDuration.Seconds())

	return nil
}
