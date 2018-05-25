package parser

import (
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/RedisLabs/rsbench/indexer"

	"github.com/RedisLabs/redisearch-go/redisearch"

	"encoding/json"

	"log"

	"strconv"
)

type timestamp int64

func (t *timestamp) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	var i int64
	if i, err = strconv.ParseInt(s, 10, 64); err == nil {
		*t = timestamp(i)
	}

	return err
}

type redditDocument struct {
	Author     string    `json:"author"`
	Body       string    `json:"body"`
	Created    timestamp `json:"created_utc"`
	Id         string    `json:"id"`
	Score      int64     `json:"score"`
	Ups        int64     `json:"ups"`
	Downs      int64     `json:"downs"`
	Subreddit  string    `json:"subreddit"`
	UvoteRatio float32   `json:"upvote_ratio"`
}

type RedditReader struct {
	dec *json.Decoder
}

func RedditSchema() *redisearch.Schema {
	return redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("body")).
		AddField(redisearch.NewTextField("author")).
		AddField(redisearch.NewTextField("sub")).
		AddField(redisearch.NewNumericField("date"))
}

func RedditReaderOpen(r io.Reader) (indexer.DocumentReader, error) {
	return &RedditReader{
		dec: json.NewDecoder(r),
	}, nil
}

func (rr *RedditReader) Read() (doc redisearch.Document, err error) {

	var rd redditDocument
	err = rr.dec.Decode(&rd)
	if err != nil {
		log.Printf("Error decoding json: %s", err)

	} else {

		doc = redisearch.NewDocument(fmt.Sprintf("%s/%s", rd.Subreddit, rd.Id),
			float32(math.Min(1, float64(math.Max(0, float64(rd.Score)))/1000))).
			Set("body", rd.Body).
			Set("author", rd.Author).
			Set("sub", rd.Subreddit).
			Set("date", int64(rd.Created))
		//Set("ups", rd.Ups)

	}
	//close(ch)
	//}()
	return
}
