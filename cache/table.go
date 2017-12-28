package cache

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"sort"
)

type TableCache struct {
	filename string
	File     *os.File
	CacheId  int
	index    []struct {
		key    []byte
		offset uint32
		size   uint32
	}
}

func NewTableCache(filename string, cacheId int) *TableCache {
	tc := &TableCache{
		filename: filename,
		CacheId:  cacheId,
	}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	tc.File = file
	return tc
}

func (tc *TableCache) fill() {
	tc.File.Seek(-8, os.SEEK_END)
	buf := make([]byte, 8)
	tc.File.Read(buf)
	buf2 := bytes.NewBuffer(buf)
	var (
		offset    uint32
		size      uint32
		keyLength uint32
		key       []byte
	)
	binary.Read(buf2, binary.LittleEndian, &offset)
	binary.Read(buf2, binary.LittleEndian, &size)
	tc.File.Seek(int64(offset), os.SEEK_SET)
	buf = make([]byte, size)
	tc.File.Read(buf)
	buf2 = bytes.NewBuffer(buf)
	for buf2.Len() != 0 {
		binary.Read(buf2, binary.LittleEndian, &keyLength)
		binary.Read(buf2, binary.LittleEndian, &offset)
		binary.Read(buf2, binary.LittleEndian, &size)
		key = make([]byte, keyLength)
		binary.Read(buf2, binary.LittleEndian, key)
		tc.index = append(tc.index, struct {
			key          []byte
			offset, size uint32
		}{key, offset, size})
	}
}

func (tc *TableCache) Get(key []byte) (offset uint32, size uint32) {
	i := sort.Search(len(tc.index), func(i int) bool { return bytes.Compare(tc.index[i].key, key) != -1 })
	if i < len(tc.index) {
		offset, size = tc.index[i].offset, tc.index[i].size
		return
	}
	log.Panic("not found")
	return
}
