package cache

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
)

type TableCache struct {
	file    *os.File
	indexs  Indexs
	cacheId int
}

type Index struct {
	key    []byte
	offset uint32
	size   uint32
}

type Indexs []Index

func (s Indexs) Len() int           { return len(s) }
func (s Indexs) Swap(i, j int)      { s[i], j[j] = s[j], s[i] }
func (s Indexs) Less(i, j int) bool { return bytes.Compare(s[i].key, s[j].key) == -1 }

func NewTableCache(filename string) *TableCache {
	tc := &TableCache{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	tc.file = file
	return tc
}

func (tc *TableCache) readIndexBlock() {
	tc.file.Seek(-8, os.SEEK_END)
	buf := make([]byte, 8)
	tc.file.Read(buf)
	buf2 := bytes.NewBuffer(buf)
	var (
		offset uint32
		size   uint32
	)
	binary.Read(buf2, binary.LittleEndian, &offset)
	binary.Read(buf2, binary.LittleEndian, &size)
	tc.file.Seek(offset, os.SEEK_SET)
	buf = make([]byte, size)
	tc.file.Read(buf)
	buf2 = bytes.NewBuffer(buf)
	for buf2.Len() != 0 {
		binary.Read(buf2, binary.LittleEndian, &keyLength)
		binary.Read(buf2, binary.LittleEndian, &offset)
		binary.Read(buf2, binary.LittleEndian, &size)
		key := make([]byte, keyLength)
		binary.Read(buf2, binary.LittleEndian, key)
		index := &Index{
			key:    key,
			offset: offset,
			size:   size,
		}
		tc.index = append(tc.index, index)
	}
	sort.Sort(tc.indexs)
}

func (tc *TableCache) Get(key []byte) []byte {
	i := sort.Search(len(tc.indexs), func(i int) bool { return bytes.Compare(tc.indexs[i].key, key) != -1 })
}
