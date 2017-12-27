package table

import (
	"container/list"
	"os"

	"github.com/tddhit/tools/log"
)

const (
	MAX_DATABLOCK_SIZE = 32
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
	sst.dataBlock = NewDataBlock(file, 0)
	sst.dataBlocks.PushBack(sst.dataBlock)
	return sst
}

func (s *SSTable) Add(key, value []byte) {
	tsize := s.dataBlockSize + len(key) + len(value) + 8
	if tsize < MAX_DATABLOCK_SIZE {
		log.Debug("less block:", tsize, MAX_DATABLOCK_SIZE)
		s.dataBlock.Add(key, value)
		s.dataBlockSize += len(key) + len(value) + 8
	} else if tsize == MAX_DATABLOCK_SIZE {
		log.Debug("equal block:", tsize, MAX_DATABLOCK_SIZE)
		s.dataBlock.Add(key, value)
		s.dataBlock.Finish()
		s.dataBlock = NewDataBlock(s.file, uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE))
		s.dataBlocks.PushBack(s.dataBlock)
		s.dataBlockSize = 0
	} else {
		log.Debug("more block:", tsize, MAX_DATABLOCK_SIZE)
		s.dataBlock.Finish()
		s.dataBlock = NewDataBlock(s.file, uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE))
		s.dataBlocks.PushBack(s.dataBlock)
		s.dataBlock.Add(key, value)
		s.dataBlockSize = len(key) + len(value) + 8
	}
}

func (s *SSTable) Finish() {
	log.Debug("len:", s.dataBlocks.Len())
	s.dataBlock.Finish()
	s.indexBlock = NewIndexBlock(s.file, uint32(s.dataBlocks.Len()*MAX_DATABLOCK_SIZE))
	for elem := s.dataBlocks.Front(); elem != nil; elem = elem.Next() {
		if dataBlock, ok := elem.Value.(*DataBlock); ok {
			s.indexBlock.Add(dataBlock.minKey, dataBlock.offset, dataBlock.size)
			log.Debug(string(dataBlock.minKey), dataBlock.offset, dataBlock.size)
		}
	}
	s.indexBlock.Finish()
	s.footer = NewFooter(s.file)
	s.footer.Add(s.indexBlock.offset, s.indexBlock.size)
	s.footer.Finish()
	s.file.Close()
}
