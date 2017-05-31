package main

import (
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/castisdev/cdn-simul/glblog"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

func main() {
	dbdir := flag.String("db-dir", "", "db directory")
	sid := flag.String("sid", "", "session id")
	prefix := flag.String("iter-prefix", "", "level db bytes prefix")
	flag.Parse()

	db, err := leveldb.OpenFile(*dbdir, nil)
	if err != nil {
		log.Fatal(err)
	}

	sout, _ := os.Create("session.csv")
	defer sout.Close()
	cout, _ := os.Create("chunk.csv")
	defer cout.Close()
	out, _ := os.Create("all.csv")
	defer out.Close()

	var iter iterator.Iterator
	if *prefix != "" {
		iter = db.NewIterator(util.BytesPrefix([]byte(*prefix)), nil)
	} else {
		iter = db.NewIterator(nil, nil)
	}
	prevHour := -1
	for iter.Next() {
		reader := bytes.NewReader(iter.Value())
		dec := gob.NewDecoder(reader)
		var e glblog.Event
		err := dec.Decode(&e)
		if err != nil {
			log.Fatal(err)
		}
		if e.EventTime.Hour() != prevHour {
			log.Println("processing,", e.EventTime)
			prevHour = e.EventTime.Hour()
		}
		if *sid == "" || *sid == e.SID {
			if e.EventType == glblog.SessionClosed || e.EventType == glblog.SessionCreated {
				fmt.Fprintln(sout, e)
			} else {
				fmt.Fprintln(cout, e)
			}
			fmt.Fprintln(out, e)
		}
	}
}
