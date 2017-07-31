package parser

import (
	"encoding/xml"
	"io"
	"path"
	"strings"

	"github.com/RedisLabs/redisearch-go/redisearch"
	"github.com/RedisLabs/rsbench/indexer"
)

func filter(title, body string) bool {

	if strings.HasPrefix(title, "List of") || strings.HasPrefix(body, "#REDIRECT") || strings.HasPrefix(body, "#redirect") ||
		strings.Contains(title, "(disambiguation)") {
		//fmt.Println(title, body)
		return false
	}

	return true
}

type WikipediaAbstractsReader struct {
	dec *xml.Decoder
}

func NewWikipediaAbstractReader(r io.Reader) *WikipediaAbstractsReader {
	return &WikipediaAbstractsReader{
		dec: xml.NewDecoder(r),
	}
}

func (wr *WikipediaAbstractsReader) Read() (doc redisearch.Document, err error) {

	var tok xml.Token
	props := map[string]string{}
	var currentText string
	for err != io.EOF {

		tok, err = wr.dec.RawToken()

		switch t := tok.(type) {

		case xml.CharData:
			if len(t) > 1 {
				currentText += string(t)
			}

		case xml.EndElement:
			name := t.Name.Local
			if name == "title" || name == "url" || name == "abstract" {
				props[name] = currentText
			} else if name == "doc" {

				id := path.Base(props["url"])
				if len(id) > 0 {
					title := strings.TrimPrefix(strings.TrimSpace(props["title"]), "Wikipedia: ")
					body := strings.TrimSpace(props["abstract"])
					//fmt.Println(title)
					if filter(title, body) {
						doc = redisearch.NewDocument(id, 1).
							Set("title", title).
							Set("body", body).
							Set("url", strings.TrimSpace(props["url"]))
							//Set("score", rand.Int31n(50000))
						return
					}
				}
				props = map[string]string{}
			}
			currentText = ""
		}

	}
	return
}

func WikiReaderOpen(r io.Reader) (indexer.DocumentReader, error) {

	return NewWikipediaAbstractReader(r), nil
}
