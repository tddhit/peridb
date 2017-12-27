package table

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"log"
	"os"
)

const MAGIC = 0x12345678

type Footer struct {
	writer *bufio.Writer
	magic  uint32
}

func NewFooter(file *os.File) *Footer {
	f := &Footer{
		writer: bufio.NewWriterSize(file, MAX_BUFFER_SIZE),
		magic:  MAGIC,
	}
	return f
}

func (f *Footer) Add(offset, size uint32) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint32(offset))
	if err != nil {
		log.Fatal(err)
	}
	f.writer.Write(buf.Bytes())
	buf = new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, uint32(size))
	if err != nil {
		log.Fatal(err)
	}
	f.writer.Write(buf.Bytes())
}

func (f *Footer) Finish() {
	f.writer.Flush()
}
