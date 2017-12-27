package db

import "testing"

func TestDB(t *testing.T) {
	db := New()
	db.Put([]byte("hello1"), []byte("world1"))
}
