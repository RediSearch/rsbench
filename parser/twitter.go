package parser

import (
	"compress/bzip2"
	"io"
	"strings"

	"github.com/RedisLabs/rsbench/indexer"

	"github.com/RedisLabs/redisearch-go/redisearch"

	"encoding/json"

	"log"
)

/*
{
   "created_at":"Sun Dec 31 17:09:00 +0000 2017",
   "id":947514941380075520,
   "id_str":"947514941380075520",
   "text":"Sesungguhnya kamu pada siang hari mempunyai urusan yg panjang (banyak) (73:7)",
   "source":"\u003ca href=\"http:\/\/twittbot.net\/\" rel=\"nofollow\"\u003etwittbot.net\u003c\/a\u003e",
   "truncated":false,
   "in_reply_to_status_id":null,
   "in_reply_to_status_id_str":null,
   "in_reply_to_user_id":null,
   "in_reply_to_user_id_str":null,
   "in_reply_to_screen_name":null,
   "user":{

      "screen_name":"FaktaMoslem",
      "location":"earth",

      "followers_count":3734,

      "time_zone":"Bangkok",
      "lang":"en",

   },
   "geo":null,
   "coordinates":null,
   "place":null,
   "contributors":null,
   "is_quote_status":false,
   "quote_count":0,
   "reply_count":0,
   "retweet_count":0,
   "Likes":0,
   "entities":{
      "hashtags":[

      ],

      "user_mentions":[

      ],

   },
   "timestamp_ms":"1514740140658"
}
*/
type tweet struct {
	Body      string    `json:"text"`
	Timestamp timestamp `json:"timestamp_ms"`
	Id        string    `json:"id_str"`
	Lang      string    `json:"lang"`
	Entities  struct {
		HashTags []struct {
			Text string
		} `json:"hashtags"`
		//Mentions []string `json:"user_mentions"`
	} `json:"entities"`
	Retweeted struct {
		Likes int `json:"favorite_count"`
	} `json:"retweeted_status"`
	User struct {
		Name     string `json:"screen_name"`
		Location string `json:"location"`
		Timezone string `json:"timezone"`
	}
}

type TwitterReader struct {
	dec *json.Decoder
}

func TwitterSchema() *redisearch.Schema {
	return redisearch.NewSchema(redisearch.Options{NoFrequencies: true, NoOffsetVectors: true, NoSave: true}).
		AddField(redisearch.NewTextFieldOptions("body",
			redisearch.TextFieldOptions{NoStem: true})).
		AddField(redisearch.NewTextFieldOptions("user", redisearch.TextFieldOptions{Sortable: true, NoStem: true})).
		AddField(redisearch.NewTextFieldOptions("lang",
			redisearch.TextFieldOptions{Sortable: true, NoIndex: true})).
		AddField(redisearch.NewTagField("hashtag")).
		AddField(redisearch.NewTextFieldOptions("location",
			redisearch.TextFieldOptions{Sortable: true})).
		AddField(redisearch.NewTagField("tz")).
		AddField(redisearch.NewNumericFieldOptions("time",
			redisearch.NumericFieldOptions{Sortable: true})).
		AddField(redisearch.NewNumericFieldOptions("likes",
			redisearch.NumericFieldOptions{Sortable: true, NoIndex: true}))

}

func TwitterReaderOpen(r io.Reader) (indexer.DocumentReader, error) {

	bz := bzip2.NewReader(r)

	return &TwitterReader{
		dec: json.NewDecoder(bz),
	}, nil
}

func (rr *TwitterReader) Read() (doc redisearch.Document, err error) {

	var tw tweet
	for {
		err = rr.dec.Decode(&tw)

		if err != nil {

			if err != io.EOF {
				log.Printf("Error decoding json: %s", err)
				continue
			}

			break
		} else {

			doc = redisearch.NewDocument(tw.Id, 1).
				Set("body", tw.Body).
				Set("user", tw.User.Name).
				Set("lang", tw.Lang).
				Set("tz", tw.User.Timezone).
				Set("time", int(tw.Timestamp/1000)).
				Set("likes", tw.Retweeted.Likes)

			if tw.Entities.HashTags != nil && len(tw.Entities.HashTags) > 0 {
				tags := make([]string, 0, len(tw.Entities.HashTags))
				for _, tag := range tw.Entities.HashTags {
					tags = append(tags, tag.Text)
				}
				doc = doc.Set("hashtag", strings.Join(tags, ","))
			}

			//}
			return
		} //Set("ups", rd.Ups)

	}
	//close(ch)
	//}()
	return
}
