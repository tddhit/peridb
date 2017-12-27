package table

type Footer struct {
	magic     uint32
	indexInfo struct {
		offset uint64
		size   uint64
	}
}
