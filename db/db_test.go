package db

import (
	"strconv"
	"testing"

	"github.com/tddhit/tools/log"
)

func TestDB(t *testing.T) {
	db := New()
	for i := 1; i <= 21200; i++ {
		db.Put([]byte("hello"+strconv.Itoa(i)), []byte("world"+strconv.Itoa(i)))
	}
	db.Close()
}
