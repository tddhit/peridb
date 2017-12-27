package table

type DataBlock struct {
	writer *bufio.Writer
	minKey []byte
	offset uint32
	size   uint32
}

func NewDataBlock() *DataBlock {
	b := &DataBlock{
		writer: bufio.NewWriterSize(file, MAX_BUFFER_SIZE),
	}
}

func (b *DataBlock) Add(key, value []byte) {
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

func (b *DataBlock) Finish() {
	b.writer.Flush()
}
