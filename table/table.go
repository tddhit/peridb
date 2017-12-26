package table

import (
	"bufio"
	"log"
	"os"
)

type SSTable struct {
	filename string
	file     *os.File
	writer   *bufio.Writer
}

type Footer struct {
}

type IndexBlock struct {
}

type MetaIndex struct {
}

type DataBlock struct {
}

type MetaBlock struct {
}

func NewSSTable(filename) *SSTable {
	sst := &SSTable{}
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal("compactMemTable fail:", err)
	}
	writer := bufio.NewWriterSize(file, MAX_BUFFER_SIZE)
	sst.file = file
	sst.writer = writer
	return sst
}

func (s *SSTable) Add(key, value []byte) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint32(len(key)))
	if err != nil {
		log.Fatal(err)
	}
	s.writer.Write(buf.Bytes())
	buf = new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, uint32(len(value)))
	if err != nil {
		log.Fatal(err)
	}
	s.writer.Write(key)
	s.writer.Write(value)
	size += len(key) + len(value) + 8
}

func (s *SSTable) Finish() {
	s.file.Close()
}
