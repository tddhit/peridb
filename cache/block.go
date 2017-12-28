package cache

type BlockCache struct {
	records []record
}

type record struct {
	key   []byte
	value []byte
}

func NewBlockCache() *BlockCache {
	bc := &BlockCache{}
	return bc
}

func (bc *BlockCache) read() {
	bc.file.Seek(offset, OS.SEEK_SET)
	buf := make([]byte, bc.size)
	bc.file.Read(buf)
	buf2 := bytes.NewBuffer(buf)
	for buf2.Len() != 0 {
		binary.Read(buf2, binary.LittleEndian, &keyLength)
		binary.Read(buf2, binary.LittleEndian, &valueLength)
		key := make([]byte, keyLength)
		value := make([]byte, valueLength)
		binary.Read(buf2, binary.LittleEndian, key)
		binary.Read(buf2, binary.LittleEndian, value)
		r := &record{
			key:   key,
			value: value,
		}
		bc.records = append(bc.records, r)
	}
}

func (bc *BlockCache) Get(key []byte) int {
	i := sort.Search(len(bc.records), func(i int) bool { return bytes.Compare(bc.records[i].key, key) != -1 })
	if bytes.Equal(bc.records[i].key, key) {
		return bc.records[i].value
	}
	return nil
}
