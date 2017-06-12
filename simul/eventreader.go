package simul

import (
	"bytes"
	"encoding/gob"
	"log"

	"github.com/castisdev/cdn-simul/glblog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

// EventReader :
type EventReader interface {
	ReadEvent() *glblog.SessionInfo
}

// DBEventReader :
type DBEventReader struct {
	iter iterator.Iterator
}

// NewDBEventReader :
func NewDBEventReader(db *leveldb.DB) *DBEventReader {
	return &DBEventReader{
		iter: db.NewIterator(nil, nil),
	}
}

// ReadEvent :
func (r *DBEventReader) ReadEvent() *glblog.SessionInfo {
	if !r.iter.Next() {
		return nil
	}
	reader := bytes.NewReader(r.iter.Value())
	dec := gob.NewDecoder(reader)
	var e glblog.SessionInfo
	err := dec.Decode(&e)
	if err != nil {
		log.Fatalf("failed to decode event from DB, %v", err)
	}
	return &e
}

// TestEventReader :
type TestEventReader struct {
	curEventIdx int
	events      []*glblog.SessionInfo
}

// NewTestEventReader :
func NewTestEventReader(evt []*glblog.SessionInfo) EventReader {
	return &TestEventReader{events: evt}
}

// ReadEvent :
func (t *TestEventReader) ReadEvent() *glblog.SessionInfo {
	t.curEventIdx++
	if t.curEventIdx > len(t.events) {
		return nil
	}
	return t.events[t.curEventIdx-1]
}
