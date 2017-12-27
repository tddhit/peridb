package table

import (
	"container/list"
	"log"
	"os"
)

const (
	MAX_DATABLOCK_SIZE = 4096
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

func NewSSTable(filename) *SSTable {
	sst := &SSTable{
		filename:   filename,
		footer:     NewFooter(),
		indexBlock: NewIndexBlock(),
		dataBlock:  NewDataBlock(),
		dataBlocks: list.New(),
	}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal("NewSSTable fail:", err)
	}
	sst.file = file
	return sst
}

func (s *SSTable) Add(key, value []byte) {
	tsize = s.dataBlockSize + len(key) + len(value) + 8
	if tsize < MAX_DATABLOCK_SIZE {
		s.dataBlock.Add(key, value)
		s.dataBlockSize += len(key) + len(value) + 8
	} else if tsize == MAX_DATABLOCK_SIZE {
		s.dataBlock.Add(key, value)
		s.dataBlock.Finish()
		s.dataBlocks.PushBack(s.dataBlock)
		s.dataBlock = NewDataBlock()
		s.dataBlockSize = 0
	} else {
		s.dataBlock.Finish()
		s.dataBlocks.PushBack(s.dataBlock)
		s.dataBlock = NewDataBlock()
		s.dataBlock.Add(key, value)
		s.blockSize = len(key) + len(value) + 8
	}
}

func (s *SSTable) Finish() {
	for elem := s.dataBlocks.Front(); elem != nil; e = elem.Next() {
		if dataBlock, ok := elem.Value.(*DataBlock); ok {
			s.indexBlock.Add(dataBlock.minKey, dataBlock.offset, dataBlock.size)
		}
	}
	s.indexBlock.Finish()
	s.footer.Add(s.indexBlock.offset, s.indexBlock.size)
	s.footer.Finish()
	s.file.Close()
}
