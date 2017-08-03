package indexer

import (
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

func (fr *FolderReader) walkDir(path string, pattern string, ch chan string) {
	info, err := os.Stat(path)
	if err != nil {
		log.Printf("Could not stat path %s: %s", path, err)
	}

	if !info.IsDir() {
		ch <- path
		return
	}

	files, err := ioutil.ReadDir(path)

	if err != nil {
		log.Printf("Could not read path %s: %s", path, err)
		panic(err)
	}

	for _, file := range files {
		fullpath := filepath.Join(path, file.Name())
		if file.IsDir() {
			fr.walkDir(fullpath, pattern, ch)
			continue
		}

		if match, err := filepath.Match(pattern, file.Name()); err == nil {

			if match {
				log.Println("Found file", fullpath)
				ch <- fullpath
			}
		} else {
			panic(err)
		}

	}
}

func (fr *FolderReader) readOne(f string, wg *sync.WaitGroup, waitch chan interface{}, ch chan<- redisearch.Document) (err error) {
	// send something to the waitch that will be consumed by the workers
	log.Println("Opening", f)
	fp, err := os.Open(f)

	if err != nil {
		log.Println("Error opening ", f, ":", err)
		return
	}

	go func(r io.Reader) {
		// defer reading from the wait channel to signal that we've finished
		defer func() {
			wg.Done()
			<-waitch
		}()

		dr, err := fr.opener.Open(fp)
		if err != nil {
			log.Println(err)
			return
		}

		for err == nil {
			doc, e := dr.Read()
			if e == nil {
				ch <- doc
			} else {
				log.Printf("Received error %s\n", e.Error())
			}
			err = e
		}
	}(fp)

	return nil
}

func (fr *FolderReader) Start(ch chan<- redisearch.Document) error {
	filech := make(chan string, 100)
	waitch := make(chan interface{}, fr.concurrency)
	donech := make(chan interface{}, 1)
	go func() {
		defer close(filech)
		fr.walkDir(fr.folder, fr.pattern, filech)
	}()

	wg := sync.WaitGroup{}

	go func() {
		for f := range filech {
			if err := fr.readOne(f, &wg, waitch, ch); err == nil {
				wg.Add(1)
				// Add a slot to waitch, which is removed by the function we
				// just called.
				waitch <- nil
			}
		}
		donech <- nil
	}()

	go func() {
		// Wait until we know for sure that no more workers will be spawned
		<-donech

		// Wait for all the spawned workers to be done.
		wg.Wait()

		// And close the document channel once there are no more documents to write
		close(ch)
	}()

	return nil
}

func (fr *FolderReader) Stop() {

}
