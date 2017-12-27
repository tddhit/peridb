package db

import (
	"fmt"

	"github.com/tddhit/peridb/table"
	"github.com/tddhit/tools/log"
	"github.com/tddhit/tools/skiplist"
)

const (
	MAX_MEMTABLE_SIZE   = 128
	LEVEL0_SSTABLE_SIZE = 64
)

type DB struct {
	mem            *skiplist.SkipList
	imm            *skiplist.SkipList
	seq            uint32
	compactionChan chan struct{}
}

func New() *DB {
	db := &DB{
		mem:            skiplist.New(),
		seq:            1,
		compactionChan: make(chan struct{}, 1),
	}
	return db
}

func (db *DB) Close() {
	select {
	case <-db.compactionChan:
		log.Debug("close.")
	}
}

func (db *DB) Get(key []byte) ([]byte, error) {
	return db.mem.Get(key), nil
}

func (db *DB) Put(key, value []byte) error {
	db.makeRoom()
	db.mem.Put(key, value)
	return nil
}

func (db *DB) makeRoom() {
	if db.mem.Size() < MAX_MEMTABLE_SIZE {
		log.Debug("return:", db.mem.Size(), MAX_MEMTABLE_SIZE)
		return
	}
	log.Debug("compact:", db.mem.Size(), MAX_MEMTABLE_SIZE)
	db.imm = db.mem
	db.mem = skiplist.New()
	go db.bgCompaction()
}

func (db *DB) bgCompaction() {
	if db.imm != nil {
		db.compactMemTable()
		//close(db.compactionChan)
		return
	}
	//close(db.compactionChan)
}

func (db *DB) compactMemTable() {
	size := 0
	filename := fmt.Sprintf("sst_%d", db.seq)
	db.seq++
	sst := table.NewSSTable(filename)
	iter := db.imm.Iterator()
	for iter.First(); !iter.End(); iter.Next() {
		tsize := size + len(iter.Key()) + len(iter.Value()) + 8
		log.Debug(string(iter.Key()), string(iter.Value()))
		if tsize < LEVEL0_SSTABLE_SIZE {
			log.Debug("less sst:", tsize, LEVEL0_SSTABLE_SIZE)
			sst.Add(iter.Key(), iter.Value())
			size += len(iter.Key()) + len(iter.Value()) + 8
		} else if tsize == LEVEL0_SSTABLE_SIZE {
			log.Debug("equal sst:", tsize, LEVEL0_SSTABLE_SIZE)
			sst.Add(iter.Key(), iter.Value())
			sst.Finish()
			filename := fmt.Sprintf("sst_%d", db.seq)
			db.seq++
			sst = table.NewSSTable(filename)
			size = 0
		} else {
			log.Debug("more sst:", tsize, LEVEL0_SSTABLE_SIZE)
			sst.Finish()
			filename := fmt.Sprintf("sst_%d", db.seq)
			db.seq++
			sst = table.NewSSTable(filename)
			sst.Add(iter.Key(), iter.Value())
			size = len(iter.Key()) + len(iter.Value()) + 8
		}
	}
	if sst != nil {
		sst.Finish()
	}
}
