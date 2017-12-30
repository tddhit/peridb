package table

import (
	"bytes"
	"container/list"
	"os"

	"github.com/tddhit/tools/log"
)

const (
	MAX_DATABLOCK_SIZE = 32
)

type SSTable struct {
	Level         int
	Filename      string
	file          *os.File
	footer        *Footer
	indexBlock    *IndexBlock
	dataBlocks    *list.List
	dataBlock     *DataBlock
	dataBlockSize int
	MinKey        []byte
	MaxKey        []byte
}

func NewSSTable(filename string) *SSTable {
	sst := &SSTable{
		Filename:   filename,
		dataBlocks: list.New(),
	}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal("NewSSTable fail:", err)
	}
	sst.file = file
	sst.dataBlock = NewDataBlock(file, 0)
	sst.dataBlocks.PushBack(sst.dataBlock)
	return sst
}

func (s *SSTable) Add(key, value []byte) {
	if s.MinKey == nil || bytes.Compare(key, s.MinKey) == -1 {
		s.MinKey = key
	}
	if s.MaxKey == nil || bytes.Compare(key, s.MaxKey) == 1 {
		s.MaxKey = key
	}
	tsize := s.dataBlockSize + len(key) + len(value) + 8
	if tsize < MAX_DATABLOCK_SIZE {
		s.dataBlock.Add(key, value)
		s.dataBlockSize += len(key) + len(value) + 8
	} else if tsize == MAX_DATABLOCK_SIZE {
		s.dataBlock.Add(key, value)
		s.dataBlock.Finish()
		s.dataBlock = NewDataBlock(s.file, uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE))
		s.dataBlocks.PushBack(s.dataBlock)
		s.dataBlockSize = 0
	} else {
		s.dataBlock.Finish()
		s.dataBlock = NewDataBlock(s.file, uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE))
		s.dataBlocks.PushBack(s.dataBlock)
		s.dataBlock.Add(key, value)
		s.dataBlockSize = len(key) + len(value) + 8
	}
}

func (s *SSTable) Finish() {
	s.dataBlock.Finish()
	s.indexBlock = NewIndexBlock(s.file, uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE))
	for elem := s.dataBlocks.Front(); elem != nil; elem = elem.Next() {
		if dataBlock, ok := elem.Value.(*DataBlock); ok {
			s.indexBlock.Add(dataBlock.minKey, dataBlock.offset, dataBlock.size)
		}
	}
	s.indexBlock.Finish()
	s.footer = NewFooter(s.file)
	s.footer.Add(s.indexBlock.offset, s.indexBlock.size)
	s.footer.Finish()
	s.file.Close()
}

func (s *SSTable) Drop() {
	s.file.Close()
}
