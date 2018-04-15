package parser

import (
	"encoding/xml"
	"io"
	"regexp"
	"strings"
	"time"

	"github.com/RedisLabs/redisearch-go/redisearch"
	"github.com/RedisLabs/rsbench/indexer"
	"github.com/grokify/html-strip-tags-go"
)

func StackSchema() *redisearch.Schema {
	return redisearch.NewSchema(redisearch.Options{NoFrequencies: true, NoOffsetVectors: true}).
		AddField(redisearch.NewTextField("body")).
		AddField(redisearch.NewTextField("title")).
		AddField(redisearch.NewTagField("tags")).
		AddField(redisearch.NewNumericFieldOptions("score",
			redisearch.NumericFieldOptions{Sortable: true, NoIndex: true})).
		AddField(redisearch.NewNumericFieldOptions("answers",
			redisearch.NumericFieldOptions{Sortable: true, NoIndex: true})).
		AddField(redisearch.NewNumericFieldOptions("time",
			redisearch.NumericFieldOptions{Sortable: true}))
}

type StackExchangeReader struct {
	dec *xml.Decoder
}

func NewStackExchangeReader(r io.Reader) *StackExchangeReader {

	return &StackExchangeReader{
		dec: xml.NewDecoder(r),
	}
}

/*<row Id="4" PostTypeId="1" AcceptedAnswerId="7" CreationDate="2008-07-31T21:42:52.667" Score="543" ViewCount="34799" Body="&lt;p&gt;I want to use a track-bar to change a form's
 opacity.&lt;/p&gt;&#xA;&#xA;&lt;p&gt;This is my code:&lt;/p&gt;&#xA;&#xA;&lt;pre&gt;&lt;code&gt;decimal trans = trackBar1.Value / 5000;&#xA;this.Opacity = trans;&#xA;&lt;/code&g
t;&lt;/pre&gt;&#xA;&#xA;&lt;p&gt;When I build the application, it gives the following error:&lt;/p&gt;&#xA;&#xA;&lt;blockquote&gt;&#xA;  &lt;p&gt;Cannot implicitly convert type &
lt;code&gt;'decimal'&lt;/code&gt; to &lt;code&gt;'double'&lt;/code&gt;.&lt;/p&gt;&#xA;&lt;/blockquote&gt;&#xA;&#xA;&lt;p&gt;I tried using &lt;code&gt;trans&lt;/code&gt; and &lt;c
ode&gt;double&lt;/code&gt; but then the control doesn't work. This code worked fine in a past VB.NET project.&lt;/p&gt;&#xA;" OwnerUserId="8" LastEditorUserId="3151675" LastEdito
rDisplayName="Rich B" LastEditDate="2017-09-27T05:52:59.927" LastActivityDate="2018-02-22T16:40:13.577" Title="While applying opacity to a form, should we use a decimal or a doub
le value?" Tags="&lt;c#&gt;&lt;winforms&gt;&lt;type-conversion&gt;&lt;decimal&gt;&lt;opacity&gt;" AnswerCount="13" CommentCount="1" FavoriteCount="39" CommunityOwnedDate="2012-10
-31T16:42:47.213" />*/

func processTags(raw string) string {
	matches := regexp.MustCompile("<([^>]+)>").FindAllString(raw, -1)
	s := make([]string, len(matches))
	for i, m := range matches {
		s[i] = strings.Trim(m, "<>")
	}
	return strings.Join(s, ",")
}
func parseAttrs(attrs []xml.Attr) (doc redisearch.Document) {
	m := map[string]string{}
	for _, a := range attrs {
		m[a.Name.Local] = a.Value
	}
	doc = redisearch.NewDocument(m["Id"], 1)
	dt, _ := time.Parse("2006-01-02T15:04:05.000", m["CreationDate"])
	doc = doc.Set("body", strip.StripTags(m["Body"])).
		Set("title", m["Title"]).
		Set("tags", processTags(m["Tags"])).
		Set("score", m["Score"]).
		Set("answers", m["AnswerCount"]).
		Set("time", dt.Unix())
	return

}
func (wr *StackExchangeReader) Read() (doc redisearch.Document, err error) {

	var tok xml.Token

	for err != io.EOF {
		//fmt.Println("Reading...")
		tok, err = wr.dec.RawToken()
		switch t := tok.(type) {

		case xml.StartElement:
			if t.Name.Local == "row" {
				doc = parseAttrs(t.Attr)
				return
			}
		default:
			continue
		}

	}
	return
}

func StackExchangeReaderOpen(r io.Reader) (indexer.DocumentReader, error) {

	return NewStackExchangeReader(r), nil
}
