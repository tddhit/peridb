package table

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"os"
)

type DataBlock struct {
	writer *bufio.Writer
	minKey []byte
	offset uint32
	size   uint32
}

func NewDataBlock(file *os.File, offset uint32) *DataBlock {
	b := &DataBlock{
		writer: bufio.NewWriterSize(file, MAX_DATABLOCK_SIZE),
		offset: offset,
	}
	return b
}

func (b *DataBlock) Add(key, value []byte) {
	if b.minKey == nil {
		b.minKey = key
	}
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint32(len(key)))
	if err != nil {
		log.Fatal(err)
	}
	b.writer.Write(buf.Bytes())
	buf = new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, uint32(len(value)))
	if err != nil {
		log.Fatal(err)
	}
	b.writer.Write(buf.Bytes())
	b.writer.Write(key)
	b.writer.Write(value)
	b.size += uint32(len(key)) + uint32(len(value)) + 8
}

func (b *DataBlock) Finish() {
	b.writer.Flush()
}
