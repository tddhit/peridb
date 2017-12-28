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
	MAX_NUM_LEVEL       = 7
)

type DB struct {
	manifest     *os.File
	mem          *skiplist.SkipList
	imm          *skiplist.SkipList
	sstableId    int
	tableCacheId int
	tableCaches  map[string]*cache.TableCache
	blockCaches  map[string]*cache.BlockCache
	levels       [MAX_NUM_LEVEL][]struct {
		filename string
		level    int
		minKey   []byte
		maxKey   []byte
	}
}

func New() *DB {
	db := &DB{
		mem:         skiplist.New(),
		tableCaches: make(map[string]*cache.TableCache),
		blockCaches: make(map[string]*cache.BlockCache),
	}
	return db
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	if db.mem != nil {
		value = db.mem.Get(key)
	}
	if value != nil {
		return
	}
	if db.imm != nil {
		value = db.imm.Get(key)
	}
	if value != nil {
		return
	}
	for i := 0; i < len(db.levels); i++ {
		level := db.levels[i]
		for _, v := range level {
			if bytes.Compare(key, v.minKey) == -1 || bytes.Compare(key, v.maxKey) == 1 {
				continue
			}
			var (
				tc *cache.TableCache
				bc *cache.BlockCache
			)
			if tc, ok := db.tableCaches[v.filename]; !ok {
				tc = cache.NewTableCache(v.filename, db.tableCacheId)
				db.tableCacheId++
				db.tableCaches[v.filename] = tc
			}
			offset, size := tc.Get(key)
			blockId := fmt.Sprintf("%d_%d", tc.CacheId, offset)
			if bc, ok := db.blockCaches[blockId]; !ok {
				bc = cache.NewBlockCache(tc.File, offset, size)
				db.blockCaches[blockId] = bc
			}
			value = bc.Get(key)
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
	filename := fmt.Sprintf("sst_%d", db.sstableId)
	db.sstableId++
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
			db.save(sst)
			filename := fmt.Sprintf("sst_%d", db.sstableId)
			db.sstableId++
			sst = table.NewSSTable(filename)
			size = 0
		} else {
			log.Debug("more sst:", tsize, LEVEL0_SSTABLE_SIZE)
			sst.Finish()
			db.save(sst)
			filename := fmt.Sprintf("sst_%d", db.sstableId)
			db.sstableId++
			sst = table.NewSSTable(filename)
			sst.Add(iter.Key(), iter.Value())
			size = len(iter.Key()) + len(iter.Value()) + 8
		}
	}
	if sst != nil {
		sst.Finish()
		db.save(sst)
	}
}

func (db *DB) save(sst *table.SSTable) {
	db.levels[sst.Level] = append(db.levels[sst.Level], struct {
		filename       string
		level          int
		minKey, maxKey []byte
	}{sst.Filename, sst.Level, sst.MinKey, sst.MaxKey})
	record := fmt.Sprintf("level_%d,%s,%s,%s\n", sst.Level, sst.Filename, sst.MinKey, sst.MaxKey)
	db.manifest.WriteString(record)
	db.manifest.Sync()
}
