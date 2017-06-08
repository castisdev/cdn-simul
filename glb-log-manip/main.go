package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/castisdev/cdn-simul/glblog"
	"github.com/castisdev/cdn-simul/vodlog"
	humanize "github.com/dustin/go-humanize"
	"github.com/syndtr/goleveldb/leveldb"
)

var chunkSize float64 = 2 * 1024 * 1024
var avgBitrate int

func main() {
	sdir := flag.String("sdir", "", "source directory")
	sdbfn := flag.String("sdb", "sid.db", "session db")
	assetOnly := flag.Bool("asset-only", false, "make only asset data")
	flag.Parse()

	var db *leveldb.DB
	var err error

	if *assetOnly == false {
		db, err = leveldb.OpenFile("session.db", nil)
		if err != nil {
			log.Fatal(err)
		}
	}

	sdb, err := leveldb.OpenFile(*sdbfn, nil)
	if err != nil {
		log.Fatal(err)
	}

	files := glblog.ListLogFiles(*sdir)
	sort.Sort(glblog.LogFileInfoSorter(files))

	fmap := make(map[string]int)

	var totalBitrate int64
	fin, err := os.Open("files.csv")
	if err == nil {
		s := bufio.NewScanner(fin)
		for s.Scan() {
			line := s.Text()
			strs := strings.Split(line, ",")
			filename := strs[0]
			bitrate, _ := strconv.Atoi(strings.TrimSpace(strs[1]))
			fmap[filename] = bitrate
			totalBitrate += int64(bitrate)
		}
	}
	avgBitrate = int(totalBitrate / int64(len(fmap)))

	var n int
	if files[len(files)-1].Date.Sub(files[0].Date) > 7*24*time.Hour {
		n = 1 //runtime.GOMAXPROCS(0) + 2
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
			smap := make(map[string]*glblog.SessionInfo)
			for i, lfi := range thisFiles {
				doOneFile(lfi.Fpath, smap, fmapLocal, db, sdb, *assetOnly)
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

	sout, _ := os.Create("sessions.csv")
	defer sout.Close()
	iter := db.NewIterator(nil, nil)
	for iter.Next() {
		reader := bytes.NewReader(iter.Value())
		dec := gob.NewDecoder(reader)
		var si glblog.SessionInfo
		err := dec.Decode(&si)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintln(sout, si)
	}

	log.Println("bye")
}

func calcAvgBitrate(fmap map[string]int) int {
	var totalBitrate int64
	for _, b := range fmap {
		totalBitrate += int64(b)
	}
	return int(totalBitrate / int64(len(fmap)))
}

////////////////////////////////////////////////////////////////////////////////

var layout = "2006-01-02 15:04:05.000"
var dateLayout = "2006-01-02"

var mu sync.Mutex

func doOneFile(fpath string, smap map[string]*glblog.SessionInfo, fmap map[string]int, db, sdb *leveldb.DB, assetOnly bool) {
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
	// totalCnt := 0
	var lastEvent *glblog.Event
	for s.Scan() {
		line := s.Text()
		if strings.Contains(line, "cueTone") {
			continue
		}

		if strings.Contains(line, "Successfully New Setup Session") ||
			strings.Contains(line, "Successfully New SemiSetup Session") {
			strs := strings.SplitN(line, ",", 8)
			si := &glblog.SessionInfo{}
			strStarted := strs[2] + " " + strs[3]
			si.Started, _ = time.ParseInLocation(layout, strStarted, loc)

			logLine := strings.Trim(strs[7], `"`)

			sepFunc := func(c rune) bool {
				return c == '[' || c == ']' || c == ',' || c == ' '
			}

			idx := strings.Index(logLine, "SessionId")
			strs2 := strings.FieldsFunc(logLine[idx:], sepFunc)
			si.SID = strs2[1]

			idx = strings.Index(logLine, "AssetID")
			strs2 = strings.FieldsFunc(logLine[idx:], sepFunc)
			si.Filename = strs2[1]

			idx = strings.Index(logLine, "Bandwidth")
			if idx != -1 {
				strs2 = strings.FieldsFunc(logLine[idx:], sepFunc)
				si.Bandwidth, err = strconv.Atoi(strs2[1])
				if err != nil {
					log.Println(err, "invalid log line, ", logLine)
					continue
				}
				if si.Bandwidth != 0 {
					fmap[si.Filename] = si.Bandwidth
				}
			} else if bw, ok := fmap[si.Filename]; ok {
				si.Bandwidth = bw
			} else if avgBitrate != 0 {
				si.Bandwidth = avgBitrate
			} else {
				si.Bandwidth = calcAvgBitrate(fmap)
			}

			if assetOnly == false {
				smap[si.SID] = si
			}
		} else if strings.Contains(line, "OnTeardownNotification") && assetOnly == false {
			strs := strings.SplitN(line, ",", 8)
			strEnded := strs[2] + " " + strs[3]

			logLine := strings.Trim(strs[7], `"`)
			strs2 := strings.Split(logLine, ",")
			sid := strings.TrimSpace(strs2[1])

			if si, ok := smap[sid]; ok {
				delete(smap, sid)
				si.Ended, _ = time.ParseInLocation(layout, strEnded, loc)

				{
					data, err := sdb.Get([]byte(sid), nil)
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
					sort.Sort(vodlog.Sorter(logs))
					if len(logs) == 1 {
						si.Offset = logs[0].StartOffset
					} else if len(logs) > 1 {
						var minDiff float64
						for _, l := range logs {
							diff := math.Abs(float64(si.Ended.Sub(l.EventTime)))
							if minDiff == 0 || diff < minDiff {
								minDiff = diff
								if l.StartOffset != -1 {
									si.Offset = l.StartOffset
								}
							}
						}
					}

					var buf bytes.Buffer
					enc := gob.NewEncoder(&buf)
					err = enc.Encode(si)
					if err != nil {
						log.Fatal(err)
					}
					batch.Put([]byte(si.Started.Format(layout)+si.SID), buf.Bytes())
				}

				// {
				// 	lastEvent = writeEvent(batch, si.sid, si.started, glblog.SessionCreated, si.filename, 0)
				// 	cnt++
				// 	totalCnt++
				// }
				// {
				// 	lastEvent = writeEvent(batch, si.sid, si.ended, glblog.SessionClosed, si.filename, 0)
				// 	cnt++
				// 	totalCnt++
				// }

				// chunkDur := time.Duration(chunkSize / (float64(si.bandwidth) / 8) * float64(time.Second.Nanoseconds()))
				// i := 0
				// for t := si.started; si.ended.Sub(t) > 0; t, i = t.Add(chunkDur), i+1 {
				// 	{
				// 		lastEvent = writeEvent(batch, si.sid, t, glblog.ChunkCreated, si.filename, i)
				// 		cnt++
				// 		totalCnt++
				// 	}
				// 	{
				// 		et := t.Add(chunkDur)
				// 		if si.ended.Sub(et) < 0 {
				// 			et = si.ended
				// 		}
				// 		lastEvent = writeEvent(batch, si.sid, et, glblog.ChunkClosed, si.filename, i)
				// 		cnt++
				// 		totalCnt++
				// 	}
				// }
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
		if lastEvent != nil {
			log.Printf("batched with %s, %s events, etime[%s]\n", filepath.Base(fpath), humanize.Comma(int64(cnt)), lastEvent.EventTime)
		}
	}

	if err := s.Err(); err != nil {
		log.Println(err)
	}
}

func writeEvent(batch *leveldb.Batch, sid string, tm time.Time, etype glblog.EventType, fname string, idx int) *glblog.Event {
	c := glblog.Event{
		SID:       sid,
		EventTime: tm,
		Filename:  fname,
		Index:     idx,
		EventType: etype,
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(c)
	if err != nil {
		log.Fatal(err)
	}
	batch.Put([]byte(c.EventTime.Format(layout)+c.SID+strconv.Itoa(int(c.EventType))), buf.Bytes())
	return &c
}
