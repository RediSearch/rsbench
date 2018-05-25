package indexer

import (
	"compress/bzip2"
	"compress/gzip"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/RedisLabs/redisearch-go/redisearch"
)

type FolderReader struct {
	concurrency int
	opener      DocumentReaderOpener
	folder      string
	pattern     string
}

func NewFolderReader(path, pattern string, concurrency int, opener DocumentReaderOpener) *FolderReader {
	return &FolderReader{
		concurrency: concurrency,
		opener:      opener,
		folder:      path,
		pattern:     pattern,
	}
}

func (fr *FolderReader) processDir(path string, pattern string, ch chan string, level int) {
	files, err := ioutil.ReadDir(path)

	if err != nil {
		log.Printf("Could not read path %s: %s", path, err)
		panic(err)
	}

	for _, file := range files {
		fullpath := filepath.Join(path, file.Name())
		fr.processPath(fullpath, pattern, ch, level+1)
	}
}

func (fr *FolderReader) processPath(path string, pattern string, ch chan string, level int) {
	file, err := os.Stat(path)
	if err != nil {
		log.Panicf("Couldn't stat %s: %s", path, err)
	}

	if file.IsDir() {
		fr.processDir(path, pattern, ch, level)
	} else {
		if match, err := filepath.Match(pattern, file.Name()); err == nil {
			// If there is only one file, ignore the extension!
			if level == 0 || match {
				log.Println("Found file", path)
				ch <- path
			}
		} else {
			panic(err)
		}
	}
}

func (fr *FolderReader) loop(ch chan<- redisearch.Document, in <-chan string, wg *sync.WaitGroup) {
	for f := range in {
		// send something to the waitch that will be consumed by the workers
		log.Println("Opening", f)
		var fp io.Reader
		var err error
		if fp, err = os.Open(f); err != nil {
			log.Println("Error opening ", f, ":", err)
		} else {
			ext := filepath.Ext(f)
			var compressedReader io.Reader
			switch ext {
			case "bz2":
				compressedReader = bzip2.NewReader(fp)
			case "gz":
				compressedReader, err = gzip.NewReader(fp)
				if err != nil {
					panic("Couldn't open gzip reader!")
				}
			}
			if compressedReader != nil {
				fp = compressedReader
			}
			dr, err := fr.opener.Open(fp)
			if err != nil {
				log.Println(err)
				return
			}
			for err == nil {

				doc, e := dr.Read()
				if e == nil {
					ch <- doc
				}
				err = e
			}
		}
		log.Println("Finished reading", f)
	}
	log.Println("Reader exiting")
	wg.Done()

}

func (fr *FolderReader) Start(ch chan<- redisearch.Document) error {
	filech := make(chan string)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {

		fr.processPath(fr.folder, fr.pattern, filech, 0)
		// filech is unbuffered, so we can close it when all the files have been read
		log.Println("finished dir walk, closing file channel")
		close(filech)
		wg.Done()
		wg.Wait()
		close(ch)
	}()

	// start the independent idexing workers
	for i := 0; i < fr.concurrency; i++ {
		wg.Add(1)
		go fr.loop(ch, filech, &wg)
	}

	return nil
}

func (fr *FolderReader) Stop() {

}
