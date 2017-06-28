package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/castisdev/cdn-simul/vodlog"
	"github.com/syndtr/goleveldb/leveldb"
)

func main() {
	sdir := flag.String("sdir", "", "source directory")
	flag.Parse()

	db, err := leveldb.OpenFile("elog.db", nil)
	if err != nil {
		log.Fatal(err)
	}

	err = os.RemoveAll("sid.db")
	if err != nil {
		log.Fatal(err)
	}

	sdb, err := leveldb.OpenFile("sid.db", nil)
	if err != nil {
		log.Fatal(err)
	}

	listFiles(*sdir, db, sdb)

	{
		of, err := os.Create("elog.csv")
		if err != nil {
			log.Println(err)
			return
		}
		defer of.Close()

		iter := db.NewIterator(nil, nil)
		for iter.Next() {
			reader := bytes.NewReader(iter.Value())
			dec := gob.NewDecoder(reader)
			var e vodlog.EventLog
			err := dec.Decode(&e)
			if err != nil {
				log.Fatal(err)
			}
			fmt.Fprintln(of, e)
		}
	}

	{
		of, err := os.Create("resetup.csv")
		if err != nil {
			log.Println(err)
			return
		}
		defer of.Close()

		iter := sdb.NewIterator(nil, nil)
		for iter.Next() {
			reader := bytes.NewReader(iter.Value())
			dec := gob.NewDecoder(reader)
			logs := []vodlog.EventLog{}
			err := dec.Decode(&logs)
			if err != nil {
				log.Fatal(err)
			}
			if len(logs) > 1 {
				sort.Sort(vodlog.Sorter(logs))
				for _, l := range logs {
					fmt.Fprintln(of, l)
				}
				fmt.Fprintln(of)
			}
		}
	}
}

func listFiles(sdir string, db, sdb *leveldb.DB) {
	files, err := ioutil.ReadDir(sdir)
	if err != nil {
		log.Fatal(err, sdir)
	}

	for _, f := range files {
		if f.IsDir() {
			fpath := path.Join(sdir, f.Name())
			listFiles(fpath, db, sdb)
			continue
		}

		if strings.HasPrefix(f.Name(), "EventLog[") == false ||
			strings.HasSuffix(f.Name(), ".log") == false {
			continue
		}

		doOneFile(path.Join(sdir, f.Name()), db, sdb)
	}
}

func doOneFile(fpath string, db, sdb *leveldb.DB) {
	batch := new(leveldb.Batch)

	f, err := os.Open(fpath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		strs := strings.SplitN(line, ",", 4)
		if len(strs) != 4 {
			continue
		}
		if strings.Contains(line, "tcpi_total_retrans") {
			continue
		}
		if strings.Contains(line, "AdverTisement") {
			continue
		}
		var etype int64
		var err error
		if strings.HasPrefix(strs[0], "0x") {
			etype, err = strconv.ParseInt(strs[0][2:], 16, 32)
			if err != nil {
				log.Fatal(err)
			}
		}

		major := etype & 0xffff0000
		if major != vodlog.SessionUsage {
			continue
		}
		minor := etype & 0xffff
		if minor == vodlog.Teardown || minor == vodlog.Close {
			continue
		}
		if minor != vodlog.Usage {
			continue
		}

		var e vodlog.EventLog

		tm, err := strconv.Atoi(strs[2])
		if err != nil {
			log.Fatal(err)
		}
		e.EventTime = time.Unix(int64(tm), 0)

		strs2 := strings.Split(strs[3], ",")

		e.ClientIP = strings.Trim(strs2[2], " ")

		sepFunc := func(c rune) bool {
			return c == '[' || c == ']' || c == ',' || c == ' '
		}

		idx := strings.Index(line, "SessionID")
		strs3 := strings.FieldsFunc(line[idx:], sepFunc)
		e.SID = strs3[1]

		idx = strings.Index(line, "bitrate")
		strs3 = strings.FieldsFunc(line[idx:], sepFunc)
		e.Bitrate, err = strconv.Atoi(strs3[1])
		if err != nil {
			log.Fatal(err)
		}

		idx = strings.Index(line, "filesize")
		strs3 = strings.FieldsFunc(line[idx:], sepFunc)
		e.Filesize, err = strconv.ParseInt(strs3[1], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		idx = strings.Index(line, "filename")
		strs3 = strings.FieldsFunc(line[idx:], sepFunc)
		e.Filename = strs3[1]

		if strings.HasPrefix(e.Filename, "cueTone_") || e.Filename == "test1.mpg" {
			continue
		}

		idx = strings.Index(line, "startoffset")
		strs3 = strings.FieldsFunc(line[idx:], sepFunc)
		e.StartOffset, err = strconv.ParseInt(strs3[1], 10, 64)
		if err != nil {
			log.Fatal(err)
		}

		idx = strings.Index(line, "resetup")
		strs3 = strings.FieldsFunc(line[idx:], sepFunc)
		e.Resetup = strs3[1] != "0"

		idx = strings.Index(line, "vod_ip")
		strs3 = strings.FieldsFunc(line[idx:], sepFunc)
		e.VodIP = strs3[1]

		{
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err = enc.Encode(e)
			if err != nil {
				log.Fatal(err)
			}
			batch.Put([]byte(e.EventTime.Format(vodlog.Layout)+e.VodIP+e.SID+strconv.Itoa(int(e.StartOffset))), buf.Bytes())
		}

		data, err := sdb.Get([]byte(e.SID), nil)
		if err != nil && err != leveldb.ErrNotFound {
			log.Fatal(err)
		}
		logs := []vodlog.EventLog{}
		if data != nil {
			reader := bytes.NewReader(data)
			dec := gob.NewDecoder(reader)
			err := dec.Decode(&logs)
			if err != nil {
				log.Fatal(err)
			}
		}
		logs = append(logs, e)
		sort.Sort(vodlog.Sorter(logs))

		{
			var buf bytes.Buffer
			enc := gob.NewEncoder(&buf)
			err = enc.Encode(logs)
			if err != nil {
				log.Fatal(err)
			}
			err := sdb.Put([]byte(e.SID), buf.Bytes(), nil)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	if err := s.Err(); err != nil {
		log.Println(err)
	}

	err = db.Write(batch, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("done with", fpath)
}
