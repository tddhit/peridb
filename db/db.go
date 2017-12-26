package db

import "github.com/tddhit/tools/skiplist"

const MAX_WRITE_BUFFER = 1 << 30

type DB struct {
	mem *skiplist.SkipList
	imm *skiplist.SkipList
	seq uint32
}

func New() *DB {
	db := &DB{
		mem: skiplist.New(),
		seq: 1,
	}
	return db
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (db *DB) Put(key, value []byte) error {
	db.makeRoom()
	return nil
}

func (db *DB) makeRoom() {
	if db.mem.Size() < MAX_WRITE_BUFFER {
		return
	}
	db.imm = db.mem
	db.mem = skiplist.New()
	go db.bgCompaction()
}

func (db *DB) bgCompaction() {
	if db.imm != nil {
		db.compactMemTable()
		return
	}
}

func (db *DB) compactMemTable() {
	var (
		size int      = 0
		sst  *SSTable = nil
		iter Iterator
	)
	iter = sk.Iterator()
	for iter.First(); !iter.End(); iter.Next() {
		if sst == nil {
			filename := fmt.Sprintf("sst_%d", db.seq)
			db.seq++
			sst = table.NewSSTable(filename)
		}
		tsize = size + len(iter.Key()) + len(iter.Value()) + 8
		if tsize < LEVEL0_SSTABLE_SIZE {
			sst.Add(iter.Key(), iter.Value())
			size += len(iter.Key()) + len(iter.Value()) + 8
		} else if tsize == LEVEL0_SSTABLE_SIZE {
			sst.Add(iter.Key(), iter.Value())
			sst.Finish()
			sst = nil
			size = 0
		} else {
			sst.Finish()
			filename := fmt.Sprintf("sst_%d", db.seq)
			db.seq++
			sst = table.NewSSTable(filename)
			sst.Add(iter.Key(), iter.Value())
			size = len(iter.Key()) + len(iter.Value()) + 8
		}
	}
}
