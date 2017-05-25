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
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	fmap := make(map[string]int)

	n := 2 //runtime.GOMAXPROCS(0) + 2
	size := (len(files) + n - 1) / n
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		begin, end := i*size, (i+1)*size
		if end > len(files) {
			end = len(files)
		}
		thisFiles := make([]logFileInfo, end-begin)
		copy(thisFiles, files[begin:end])
		thisDate := thisFiles[len(thisFiles)-1].date
		if i != n-1 {
			nextDate := thisDate.Add(time.Hour * 24)
			for j := end; true; j++ {
				if files[j].date.Equal(thisDate) || files[j].date.Equal(nextDate) {
					thisFiles = append(thisFiles, files[j])
				} else {
					break
				}
			}
		}
		wg.Add(1)
		go func() {
			fmapLocal := make(map[string]int)
			smap := make(map[string]*sessionInfo)
			batch := new(leveldb.Batch)
			for i, lfi := range thisFiles {
				doOneFile(lfi.fpath, smap, fmapLocal, batch)
				if i != 0 && i%10 == 0 {
					err = db.Write(batch, nil)
					if err != nil {
						log.Fatal(err)
					}
					batch = new(leveldb.Batch)
					log.Printf("batched with %s, %d/%d\n", filepath.Base(lfi.fpath), i, len(thisFiles))
				} else {
					log.Printf("done with %s, %d/%d\n", filepath.Base(lfi.fpath), i, len(thisFiles))
				}
			}
			err = db.Write(batch, nil)
			if err != nil {
				log.Fatal(err)
			}
			mu.Lock()
			for k, v := range fmapLocal {
				fmap[k] = v
			}
			wg.Done()
		}()
	}

	wg.Wait()

	iter := db.NewIterator(nil, nil)
	var scnt, ccnt int64
	for iter.Next() {
		reader := bytes.NewReader(iter.Value())
		dec := gob.NewDecoder(reader)
		var e event
		err := dec.Decode(&e)
		if err != nil {
			log.Fatal(err)
		}
		if e.EventType == sessionClosed || e.EventType == sessionCreated {
			scnt++
		} else {
			ccnt++
		}
	}
	fmt.Printf("session event length = %d, chunk event length = %d\n", scnt, ccnt)

	fout, _ := os.Create("files.csv")
	defer fout.Close()
	for k, v := range fmap {
		fmt.Fprintf(fout, "%s, %d\n", k, v)
	}
}

type logFileInfo struct {
	fpath string
	date  time.Time
	index int
}

func (l logFileInfo) String() string {
	return fmt.Sprintf("%s, %s, %d", l.fpath, l.date.Format(dateLayout), l.index)
}

type logFileInfoSorter []logFileInfo

func (lis logFileInfoSorter) Len() int {
	return len(lis)
}
func (lis logFileInfoSorter) Swap(i, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}
func (lis logFileInfoSorter) Less(i, j int) bool {
	if lis[i].date.Equal(lis[j].date) == false {
		return lis[i].date.Before(lis[j].date)
	}
	return lis[i].index < lis[j].index
}

func listLogFiles(sdir string) []logFileInfo {
	files, err := ioutil.ReadDir(sdir)
	if err != nil {
		log.Fatal(err, sdir)
	}

	loc, _ := time.LoadLocation("Local")
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
				li.date, err = time.ParseInLocation(dateLayout, strs2[0], loc)
				if err != nil {
					log.Println("invalid filename, ", li.fpath, err)
					continue
				}
				li.index, err = strconv.Atoi(strs2[1])
				if err != nil {
					log.Println("invalid filename, ", li.fpath, err)
					continue
				}
			} else {
				li.date, err = time.ParseInLocation(dateLayout, strs[0], loc)
				if err != nil {
					log.Println("invalid filename, ", li.fpath, err)
					continue
				}
				li.index = 0
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

var mu sync.Mutex

func doOneFile(fpath string, smap map[string]*sessionInfo, fmap map[string]int, batch *leveldb.Batch) {
	f, err := os.Open(fpath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

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
			fmap[si.filename] = si.bandwidth
		} else if strings.Contains(line, "OnTeardownNotification") {
			strs := strings.SplitN(line, ",", 8)
			strEnded := strs[2] + " " + strs[3]

			logLine := strings.Trim(strs[7], `"`)
			strs2 := strings.Split(logLine, ",")
			sid := strings.TrimSpace(strs2[1])

			if si, ok := smap[sid]; ok {
				delete(smap, sid)
				si.ended, _ = time.ParseInLocation(layout, strEnded, loc)
				{
					c := event{
						SID:       si.sid,
						EventTime: si.started,
						EventType: sessionCreated,
						Filename:  si.filename,
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
					c := event{
						SID:       si.sid,
						EventTime: si.ended,
						EventType: sessionClosed,
						Filename:  si.filename,
					}
					var buf bytes.Buffer
					enc := gob.NewEncoder(&buf)
					err := enc.Encode(c)
					if err != nil {
						log.Fatal(err)
					}
					batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
				}

				chunkDur := time.Duration(chunkSize / (float64(si.bandwidth) / 8) * float64(time.Second.Nanoseconds()))
				i := 0
				for t := si.started; si.ended.Sub(t) > 0; t, i = t.Add(chunkDur), i+1 {
					{
						c := event{
							SID:       si.sid,
							EventTime: t,
							Filename:  si.filename,
							Index:     i,
							EventType: chunkCreated,
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
						c := event{
							SID:       si.sid,
							EventTime: et,
							Filename:  si.filename,
							Index:     i,
							EventType: chunkClosed,
						}
						var buf bytes.Buffer
						enc := gob.NewEncoder(&buf)
						err := enc.Encode(c)
						if err != nil {
							log.Fatal(err)
						}
						batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
					}
				}
			}
		}
	}

	if err := s.Err(); err != nil {
		log.Println(err)
	}
}

////////////////////////////////////////////////////////////////////////////////

type eventType int

func (et eventType) String() string {
	switch et {
	case sessionCreated:
		return "s created"
	case sessionClosed:
		return "s closed"
	case chunkCreated:
		return "c created"
	case chunkClosed:
		return "c closed"
	default:
		return "unknown"
	}
}

const (
	chunkClosed   eventType = iota
	sessionClosed eventType = iota
	sessionCreated
	chunkCreated
)

type event struct {
	SID       string
	EventTime time.Time
	Filename  string
	Index     int
	EventType eventType
}

func (e event) String() string {
	return fmt.Sprintf("%s, %s, %9v, %4d, %s", e.EventTime.Format(layout), e.SID, e.EventType, e.Index, e.Filename)
}

type eventSorter []event

func (lis eventSorter) Len() int {
	return len(lis)
}
func (lis eventSorter) Swap(i, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}
func (lis eventSorter) Less(i, j int) bool {
	return lis[i].EventTime.Before(lis[j].EventTime)
}
