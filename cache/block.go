package cache

import (
	"bytes"
	"encoding/binary"
	"os"
	"sort"
)

type BlockCache struct {
	file    *os.File
	offset  uint32
	size    uint32
	records []struct {
		key   []byte
		value []byte
	}
}

func NewBlockCache(file *os.File, offset, size uint32) *BlockCache {
	bc := &BlockCache{
		file:   file,
		offset: offset,
		size:   size,
	}
	bc.fill()
	return bc
}

func (bc *BlockCache) fill() {
	bc.file.Seek(int64(bc.offset), os.SEEK_SET)
	buf := make([]byte, bc.size)
	bc.file.Read(buf)
	buf2 := bytes.NewBuffer(buf)
	var (
		keyLength   uint32
		valueLength uint32
		key         []byte
		value       []byte
	)
	for buf2.Len() != 0 {
		binary.Read(buf2, binary.LittleEndian, &keyLength)
		binary.Read(buf2, binary.LittleEndian, &valueLength)
		key = make([]byte, keyLength)
		value = make([]byte, valueLength)
		binary.Read(buf2, binary.LittleEndian, key)
		binary.Read(buf2, binary.LittleEndian, value)
		bc.records = append(bc.records, struct{ key, value []byte }{key, value})
	}
}

func (bc *BlockCache) Get(key []byte) []byte {
	i := sort.Search(len(bc.records), func(i int) bool { return bytes.Compare(bc.records[i].key, key) != -1 })
	if bytes.Equal(bc.records[i].key, key) {
		return bc.records[i].value
	}
	return nil
}
