package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/castisdev/cdn-simul/glblog"
	humanize "github.com/dustin/go-humanize"
	"github.com/syndtr/goleveldb/leveldb"
)

var chunkSize float64 = 2 * 1024 * 1024

func main() {
	sdir := flag.String("sdir", "", "source directory")
	assetOnly := flag.Bool("asset-only", false, "make only asset data")
	flag.Parse()

	var db *leveldb.DB
	var err error

	if *assetOnly == false {
		db, err = leveldb.OpenFile("chunk.db", nil)
		if err != nil {
			log.Fatal(err)
		}
	}

	files := glblog.ListLogFiles(*sdir)
	sort.Sort(glblog.LogFileInfoSorter(files))

	fmap := make(map[string]int)

	var n int
	if files[len(files)-1].Date.Sub(files[0].Date) > 7*24*time.Hour {
		n = 2 //runtime.GOMAXPROCS(0) + 2
	} else {
		n = 1
	}
	size := (len(files) + n - 1) / n
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		begin, end := i*size, (i+1)*size
		if end > len(files) {
			end = len(files)
		}
		thisFiles := make([]glblog.LogFileInfo, end-begin)
		copy(thisFiles, files[begin:end])
		thisDate := thisFiles[len(thisFiles)-1].Date
		if i != n-1 {
			nextDate := thisDate.Add(time.Hour * 24)
			for j := end; true; j++ {
				if files[j].Date.Equal(thisDate) || files[j].Date.Equal(nextDate) {
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
			for i, lfi := range thisFiles {
				doOneFile(lfi.Fpath, smap, fmapLocal, db, *assetOnly)
				log.Printf("done with %s, %d/%d\n", filepath.Base(lfi.Fpath), i+1, len(thisFiles))
			}
			mu.Lock()
			for k, v := range fmapLocal {
				fmap[k] = v
			}
			mu.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()
	log.Println("all events was writed")

	fout, _ := os.Create("files.csv")
	defer fout.Close()
	for k, v := range fmap {
		fmt.Fprintf(fout, "%s, %d\n", k, v)
	}
	log.Println("bye")
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

func doOneFile(fpath string, smap map[string]*sessionInfo, fmap map[string]int, db *leveldb.DB, assetOnly bool) {
	f, err := os.Open(fpath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	loc, _ := time.LoadLocation("Local")
	batch := new(leveldb.Batch)

	s := bufio.NewScanner(f)
	cnt := 0
	totalCnt := 0
	var lastEvent glblog.Event
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

			if assetOnly == false {
				smap[si.sid] = si
			}
			fmap[si.filename] = si.bandwidth
		} else if strings.Contains(line, "OnTeardownNotification") && assetOnly == false {
			strs := strings.SplitN(line, ",", 8)
			strEnded := strs[2] + " " + strs[3]

			logLine := strings.Trim(strs[7], `"`)
			strs2 := strings.Split(logLine, ",")
			sid := strings.TrimSpace(strs2[1])

			if si, ok := smap[sid]; ok {
				delete(smap, sid)
				si.ended, _ = time.ParseInLocation(layout, strEnded, loc)
				{
					c := glblog.Event{
						SID:       si.sid,
						EventTime: si.started,
						EventType: glblog.SessionCreated,
						Filename:  si.filename,
					}
					var buf bytes.Buffer
					enc := gob.NewEncoder(&buf)
					err := enc.Encode(c)
					if err != nil {
						log.Fatal(err)
					}
					batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
					cnt++
					totalCnt++
					lastEvent = c
				}
				{
					c := glblog.Event{
						SID:       si.sid,
						EventTime: si.ended,
						EventType: glblog.SessionClosed,
						Filename:  si.filename,
					}
					var buf bytes.Buffer
					enc := gob.NewEncoder(&buf)
					err := enc.Encode(c)
					if err != nil {
						log.Fatal(err)
					}
					batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
					cnt++
					totalCnt++
					lastEvent = c
				}

				chunkDur := time.Duration(chunkSize / (float64(si.bandwidth) / 8) * float64(time.Second.Nanoseconds()))
				i := 0
				for t := si.started; si.ended.Sub(t) > 0; t, i = t.Add(chunkDur), i+1 {
					{
						c := glblog.Event{
							SID:       si.sid,
							EventTime: t,
							Filename:  si.filename,
							Index:     i,
							EventType: glblog.ChunkCreated,
						}
						var buf bytes.Buffer
						enc := gob.NewEncoder(&buf)
						err := enc.Encode(c)
						if err != nil {
							log.Fatal(err)
						}
						batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
						cnt++
						totalCnt++
						lastEvent = c
					}
					{
						et := t.Add(chunkDur)
						if si.ended.Sub(et) < 0 {
							et = si.ended
						}
						c := glblog.Event{
							SID:       si.sid,
							EventTime: et,
							Filename:  si.filename,
							Index:     i,
							EventType: glblog.ChunkClosed,
						}
						var buf bytes.Buffer
						enc := gob.NewEncoder(&buf)
						err := enc.Encode(c)
						if err != nil {
							log.Fatal(err)
						}
						batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
						cnt++
						totalCnt++
						lastEvent = c
					}
				}
			}
		}

		if assetOnly == false && cnt > 1000000 {
			err = db.Write(batch, nil)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("batched with %s, %s events, etime[%s]\n", filepath.Base(fpath), humanize.Comma(int64(cnt)), lastEvent.EventTime)
			batch = new(leveldb.Batch)
			cnt = 0
		}
	}

	if assetOnly == false {
		err = db.Write(batch, nil)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("batched with %s, %s events, etime[%s]\n", filepath.Base(fpath), humanize.Comma(int64(cnt)), lastEvent.EventTime)
	}

	if err := s.Err(); err != nil {
		log.Println(err)
	}
}
