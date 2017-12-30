package table

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"

	"github.com/tddhit/tools/log"
)

type IndexBlock struct {
	writer *bufio.Writer
	offset uint32
	size   uint32
}

func NewIndexBlock(file *os.File, offset uint32) *IndexBlock {
	file.Seek(int64(offset), os.SEEK_SET)
	b := &IndexBlock{
		offset: offset,
		writer: bufio.NewWriter(file),
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
