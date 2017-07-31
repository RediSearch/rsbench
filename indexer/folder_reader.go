package indexer


type FolderReader struct {
	concurrency int
	opener      DocumentReaderOpener
	folder      string
	prefix      string
}
