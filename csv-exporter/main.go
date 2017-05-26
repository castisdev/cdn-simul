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
)

func main() {
	dbdir := flag.String("db-dir", "", "db directory")
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

	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		reader := bytes.NewReader(iter.Value())
		dec := gob.NewDecoder(reader)
		var e glblog.Event
		err := dec.Decode(&e)
		if err != nil {
			log.Fatal(err)
		}
		if e.EventType == glblog.SessionClosed || e.EventType == glblog.SessionCreated {
			fmt.Fprintln(sout, e)
		} else {
			fmt.Fprintln(cout, e)
		}
		fmt.Fprintln(out, e)
	}
}
