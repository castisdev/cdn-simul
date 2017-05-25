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

	"github.com/syndtr/goleveldb/leveldb"
)

var chunkSize float64 = 2 * 1024 * 1024

func main() {
	sdir := flag.String("sdir", "", "source directory")
	flag.Parse()

	db, err := leveldb.OpenFile("chunk.db", nil)
	if err != nil {
		log.Fatal(err)
	}

	files := listLogFiles(*sdir)
	sort.Sort(logFileInfoSorter(files))

	smap := make(map[string]*sessionInfo)
	var slist []*sessionInfo
	var elist []sessionEvent
	// var clist []chunkEvent

	for _, lfi := range files {
		doOneFile(lfi.fpath, smap, &slist, &elist, db)
	}
	sort.Sort(sessionEventSorter(elist))
	// sort.Sort(chunkEventSorter(clist))

	fmt.Printf("map length = %d, session length = %d\n", len(smap), len(slist))
	var d time.Duration
	for _, s := range slist {
		d += s.ended.Sub(s.started)
	}
	fmt.Printf("average session duration = %v\n", d/time.Duration(len(slist)))

	sout, _ := os.Create("session_event.csv")
	defer sout.Close()
	for _, e := range elist {
		fmt.Fprintln(sout, e)
	}

	cout, _ := os.Create("chunk_event.csv")
	defer cout.Close()
	iter := db.NewIterator(nil, nil)
	var cnt int64
	for iter.Next() {
		reader := bytes.NewReader(iter.Value())
		dec := gob.NewDecoder(reader)
		var c chunkEvent
		err := dec.Decode(&c)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintln(cout, c)
		cnt++
	}
	fmt.Printf("chunk length = %d\n", cnt)

	// for _, e := range elist {
	// 	fmt.Println(e)
	// }
	// for _, v := range slist {
	// 	fmt.Println(v)
	// }
}

type logFileInfo struct {
	fpath string
	date  string
	index int
}

type logFileInfoSorter []logFileInfo

func (lis logFileInfoSorter) Len() int {
	return len(lis)
}
func (lis logFileInfoSorter) Swap(i, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}
func (lis logFileInfoSorter) Less(i, j int) bool {
	if lis[i].date != lis[j].date {
		return lis[i].date < lis[j].date
	}
	return lis[i].index < lis[j].index
}

func listLogFiles(sdir string) []logFileInfo {
	files, err := ioutil.ReadDir(sdir)
	if err != nil {
		log.Fatal(err, sdir)
	}

	var logs []logFileInfo
	for _, f := range files {
		if f.IsDir() {
			fpath := path.Join(sdir, f.Name())
			logs = append(logs, listLogFiles(fpath)...)
			continue
		}

		if strings.HasSuffix(f.Name(), "_GLB.log") {
			li := logFileInfo{fpath: path.Join(sdir, f.Name())}
			strs := strings.Split(f.Name(), "_")
			if len(strs) != 2 {
				log.Println("invalid filename, ", li.fpath)
				continue
			}

			if strings.Contains(f.Name(), "[") {
				strs2 := strings.FieldsFunc(strs[0], func(c rune) bool {
					return c == '[' || c == ']'
				})
				li.date = strs2[0]
				li.index, err = strconv.Atoi(strs2[1])
				if err != nil {
					log.Println("invalid filename, ", li.fpath, err)
					continue
				}
			} else {
				li.date = strs[0]
				li.index = 0
				// YYYY-MM-DD
				if len(li.date) != 10 {
					log.Println("invalid filename, ", li.fpath)
					continue
				}
			}

			logs = append(logs, li)
		}
	}

	return logs
}

////////////////////////////////////////////////////////////////////////////////

type sessionInfo struct {
	sid       string
	started   time.Time
	ended     time.Time
	filename  string
	bandwidth int
}

var layout = "2006-01-02 15:04:05.000"
var dateLayout = "2006-01-02"

func (s sessionInfo) String() string {
	return fmt.Sprintf("%s, %s, %s, %s, %d", s.sid, s.started.Format(layout), s.ended.Format(layout), s.filename, s.bandwidth)
}

// func doOneFile(fpath string, smap map[string]*sessionInfo, slist *[]*sessionInfo, elist *[]sessionEvent, clist *[]chunkEvent) {
func doOneFile(fpath string, smap map[string]*sessionInfo, slist *[]*sessionInfo, elist *[]sessionEvent, db *leveldb.DB) {
	f, err := os.Open(fpath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	batch := new(leveldb.Batch)
	loc, _ := time.LoadLocation("Local")

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if strings.Contains(line, "cueTone") {
			continue
		}

		if strings.Contains(line, "Successfully New Setup Session") {
			strs := strings.SplitN(line, ",", 8)
			si := &sessionInfo{}
			strStarted := strs[2] + " " + strs[3]
			si.started, _ = time.ParseInLocation(layout, strStarted, loc)

			logLine := strings.Trim(strs[7], `"`)

			sepFunc := func(c rune) bool {
				return c == '[' || c == ']' || c == ',' || c == ' '
			}

			idx := strings.Index(logLine, "SessionId")
			strs2 := strings.FieldsFunc(logLine[idx:], sepFunc)
			si.sid = strs2[1]

			idx = strings.Index(logLine, "AssetID")
			strs2 = strings.FieldsFunc(logLine[idx:], sepFunc)
			si.filename = strs2[1]

			idx = strings.Index(logLine, "Bandwidth")
			strs2 = strings.FieldsFunc(logLine[idx:], sepFunc)
			si.bandwidth, err = strconv.Atoi(strs2[1])
			if err != nil {
				fmt.Println(err, "invalid log line, ", logLine)
				continue
			}

			smap[si.sid] = si
		} else if strings.Contains(line, "OnTeardownNotification") {
			strs := strings.SplitN(line, ",", 8)
			strEnded := strs[2] + " " + strs[3]

			logLine := strings.Trim(strs[7], `"`)
			strs2 := strings.Split(logLine, ",")
			sid := strings.TrimSpace(strs2[1])

			if si, ok := smap[sid]; ok {
				si.ended, _ = time.ParseInLocation(layout, strEnded, loc)
				*slist = append(*slist, si)
				delete(smap, sid)
				*elist = append(*elist, sessionEvent{
					sid:       si.sid,
					eventTime: si.started,
					eventType: created,
					filename:  si.filename,
					bandwidth: si.bandwidth,
				})
				*elist = append(*elist, sessionEvent{
					sid:       si.sid,
					eventTime: si.ended,
					eventType: closed,
					filename:  si.filename,
					bandwidth: si.bandwidth,
				})

				chunkDur := time.Duration(chunkSize / (float64(si.bandwidth) / 8) * float64(time.Second.Nanoseconds()))
				i := 0
				for t := si.started; si.ended.Sub(t) > 0; t, i = t.Add(chunkDur), i+1 {
					{
						c := chunkEvent{
							SID:       si.sid,
							EventTime: t,
							Filename:  si.filename,
							Index:     i,
							EventType: created,
						}
						var buf bytes.Buffer
						enc := gob.NewEncoder(&buf)
						err := enc.Encode(c)
						if err != nil {
							log.Fatal(err)
						}
						batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
					}
					{
						et := t.Add(chunkDur)
						if si.ended.Sub(et) < 0 {
							et = si.ended
						}
						c := chunkEvent{
							SID:       si.sid,
							EventTime: et,
							Filename:  si.filename,
							Index:     i,
							EventType: closed,
						}
						var buf bytes.Buffer
						enc := gob.NewEncoder(&buf)
						err := enc.Encode(c)
						if err != nil {
							log.Fatal(err)
						}
						batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
					}
					// *clist = append(*clist, c)
				}
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

	fmt.Println("done with", fpath)
}

////////////////////////////////////////////////////////////////////////////////

type eventType int

func (et eventType) String() string {
	switch et {
	case created:
		return "created"
	case closed:
		return "closed"
	default:
		return "unknown"
	}
}

const (
	closed eventType = iota
	created
)

type sessionEvent struct {
	sid       string
	eventTime time.Time
	eventType eventType
	filename  string
	bandwidth int
}

func (e sessionEvent) String() string {
	return fmt.Sprintf("%s, %s, %v, %s, %d", e.eventTime.Format(layout), e.sid, e.eventType, e.filename, e.bandwidth)
}

type sessionEventSorter []sessionEvent

func (lis sessionEventSorter) Len() int {
	return len(lis)
}
func (lis sessionEventSorter) Swap(i, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}
func (lis sessionEventSorter) Less(i, j int) bool {
	return lis[i].eventTime.Before(lis[j].eventTime)
}

////////////////////////////////////////////////////////////////////////////////

type chunkEvent struct {
	SID       string
	EventTime time.Time
	Filename  string
	Index     int
	EventType eventType
}

func (e chunkEvent) String() string {
	return fmt.Sprintf("%s, %s, %7v, %4d, %s", e.EventTime.Format(layout), e.SID, e.EventType, e.Index, e.Filename)
}

type chunkEventSorter []chunkEvent

func (lis chunkEventSorter) Len() int {
	return len(lis)
}
func (lis chunkEventSorter) Swap(i, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}
func (lis chunkEventSorter) Less(i, j int) bool {
	return lis[i].EventTime.Before(lis[j].EventTime)
}
