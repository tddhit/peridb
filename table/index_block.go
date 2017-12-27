package table

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"os"
)

type IndexBlock struct {
	writer *bufio.Writer
	size   uint32
}

func NewIndexBlock(file *os.File) *IndexBlock {
	b := &IndexBlock{
		writer: bufio.NewWriterSize(file, MAX_BUFFER_SIZE),
	}
	return b
}

func (b *IndexBlock) Add(key []byte, offset, size uint32) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint32(len(key)))
	if err != nil {
		log.Fatal(err)
	}
	b.writer.Write(buf.Bytes())
	buf = new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, uint32(offset))
	if err != nil {
		log.Fatal(err)
	}
	b.writer.Write(buf.Bytes())
	buf = new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, uint32(size))
	if err != nil {
		log.Fatal(err)
	}
	b.writer.Write(buf.Bytes())
	b.writer.Write(key)
	b.size += uint32(len(key)) + 12
}

func (b *IndexBlock) Finish() {
	b.writer.Flush()
}
