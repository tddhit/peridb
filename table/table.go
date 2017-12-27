package table

import (
	"container/list"
	"log"
	"os"
)

const (
	MAX_DATABLOCK_SIZE = 4096
	MAX_BUFFER_SIZE    = 4096
)

type SSTable struct {
	filename      string
	file          *os.File
	footer        *Footer
	indexBlock    *IndexBlock
	dataBlocks    *list.List
	dataBlock     *DataBlock
	dataBlockSize int
}

func NewSSTable(filename string) *SSTable {
	sst := &SSTable{
		filename:   filename,
		dataBlocks: list.New(),
	}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal("NewSSTable fail:", err)
	}
	sst.file = file
	sst.footer = NewFooter(file)
	sst.indexBlock = NewIndexBlock(file)
	sst.dataBlock = NewDataBlock(file, 0)
	return sst
}

func (s *SSTable) Add(key, value []byte) {
	tsize := s.dataBlockSize + len(key) + len(value) + 8
	if tsize < MAX_DATABLOCK_SIZE {
		s.dataBlock.Add(key, value)
		s.dataBlockSize += len(key) + len(value) + 8
	} else if tsize == MAX_DATABLOCK_SIZE {
		s.dataBlock.Add(key, value)
		s.dataBlock.Finish()
		s.dataBlocks.PushBack(s.dataBlock)
		s.dataBlock = NewDataBlock(s.file, uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE))
		s.dataBlockSize = 0
	} else {
		s.dataBlock.Finish()
		s.dataBlocks.PushBack(s.dataBlock)
		s.dataBlock = NewDataBlock(s.file, uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE))
		s.dataBlock.Add(key, value)
		s.dataBlockSize = len(key) + len(value) + 8
	}
}

func (s *SSTable) Finish() {
	for elem := s.dataBlocks.Front(); elem != nil; elem = elem.Next() {
		if dataBlock, ok := elem.Value.(*DataBlock); ok {
			s.indexBlock.Add(dataBlock.minKey, dataBlock.offset, dataBlock.size)
		}
	}
	s.indexBlock.Finish()
	s.footer.Add(uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE), s.indexBlock.size)
	s.footer.Finish()
	s.file.Close()
}
