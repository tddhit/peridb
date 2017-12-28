package db

import (
	"bytes"
	"fmt"
	"os"

	"github.com/tddhit/peridb/cache"
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
	tableCaches    map[string]struct {
		file  *os.File
		cache *cache.TableCache
	}
	blockCaches map[string]*cache.BlockCache
	levels      map[string]struct {
		minKey, maxKey []byte
	}
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

func (db *DB) Get(key []byte) (value []byte, err error) {
	if db.mem != nil {
		value = db.mem.Get(key)
	}
	if value == nil && db.imm != nil {
		value = db.imm.Get(key)
	}
	if value == nil {
		for i := 0; i < levels; i++ {
			levelCache := levels[i]
			for k, v := range levelCache {
				if bytes.Compare(key, v.minKey) != -1 && bytes.Compare(key, v.MaxKey) != 1 {
					if tableCache, ok := tableCaches[k]; ok {
					} else {
						tc := NewTableCache(k)
						cacheId = tc.Get(key)
						if blockCache, ok := blockCaches[cacheId]; ok {
							value := blockCache.Get(key)
						} else {
						}
					}
				}
			}
		}
	}
	return
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
			db.levels[sst.filename] = struct {
				minKey []byte
				maxKey []byte
			}{sst.minKey, sst.maxKey}
			filename := fmt.Sprintf("sst_%d", db.seq)
			db.seq++
			sst = table.NewSSTable(filename)
			size = 0
		} else {
			log.Debug("more sst:", tsize, LEVEL0_SSTABLE_SIZE)
			sst.Finish()
			db.levels[sst.filename] = struct {
				minKey []byte
				maxKey []byte
			}{sst.minKey, sst.maxKey}
			filename := fmt.Sprintf("sst_%d", db.seq)
			db.seq++
			sst = table.NewSSTable(filename)
			sst.Add(iter.Key(), iter.Value())
			size = len(iter.Key()) + len(iter.Value()) + 8
		}
	}
	if sst != nil {
		sst.Finish()
		db.levels[sst.filename] = struct {
			minKey []byte
			maxKey []byte
		}{sst.minKey, sst.maxKey}
	}
}
