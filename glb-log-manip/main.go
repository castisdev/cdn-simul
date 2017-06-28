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
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/castisdev/cdn-simul/glblog"
	"github.com/castisdev/cdn-simul/vodlog"
	"github.com/syndtr/goleveldb/leveldb"
)

var chunkSize float64 = 2 * 1024 * 1024
var avgBitrate int

type fileInfo struct {
	bitrate  int
	filesize int64
}

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

	fmap := make(map[string]*fileInfo)

	var totalBitrate int64
	cnt := 0
	fin, err := os.Open("files.csv")
	if err == nil {
		s := bufio.NewScanner(fin)
		for s.Scan() {
			line := s.Text()
			strs := strings.Split(line, ",")
			filename := strs[0]
			bitrate, _ := strconv.Atoi(strings.TrimSpace(strs[1]))
			if bitrate != 0 {
				cnt++
			}
			var filesize int64
			if len(strs) > 2 {
				filesize, _ = strconv.ParseInt(strings.TrimSpace(strs[2]), 10, 64)
			}
			fmap[filename] = &fileInfo{bitrate, filesize}
			totalBitrate += int64(bitrate)
		}
	}
	avgBitrate = int(totalBitrate / int64(cnt))

	smap := make(map[string]*glblog.SessionInfo)
	gmap := make(map[string]struct{})

	for i, lfi := range files {
		doOneFile(lfi.Fpath, smap, fmap, gmap, db, sdb, *assetOnly)
		log.Printf("done with %s, %d/%d\n", lfi.Fpath, i+1, len(files))
	}

	log.Println("all events was writed")

	fout, _ := os.Create("files.csv")
	defer fout.Close()
	for k, v := range fmap {
		fmt.Fprintf(fout, "%s, %d, %d\n", k, v.bitrate, v.filesize)
	}

	if *assetOnly == false {
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
	}

	log.Println("bye")
}

func calcAvgBitrate(fmap map[string]*fileInfo) int {
	var totalBitrate int64
	cnt := 0
	for _, b := range fmap {
		if b.bitrate != 0 {
			cnt++
			totalBitrate += int64(b.bitrate)
		}
	}
	return int(totalBitrate / int64(cnt))
}

////////////////////////////////////////////////////////////////////////////////

var layout = "2006-01-02 15:04:05.000"
var dateLayout = "2006-01-02"

var mu sync.Mutex

func doOneFile(fpath string, smap map[string]*glblog.SessionInfo, fmap map[string]*fileInfo,
	gmap map[string]struct{}, db, sdb *leveldb.DB, assetOnly bool) {
	f, err := os.Open(fpath)
	if err != nil {
		log.Println(err)
		return
	}
	defer f.Close()

	loc, _ := time.LoadLocation("Local")
	batch := new(leveldb.Batch)

	isCenter := strings.Contains(fpath, "center")

	sepFunc := func(c rune) bool {
		return c == '[' || c == ']' || c == ',' || c == ' ' || c == ':'
	}

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := s.Text()
		if strings.Contains(line, "cueTone") {
			continue
		}

		if strings.Contains(line, "result is file not found") {
			strs := strings.SplitN(line, ",", 8)
			logLine := strings.Trim(strs[7], `"`)
			if strings.Contains(line, "OnRetrieveBandwidthResponse") {
				// OnRetrieveBandwidthResponse : 64564ebb-abcb-4419-9e4e-1f13172139e8, 1e33290a-846b-4e48-98fd-1d6dfd46c4dd, 0, MZ4H200KSGL1500002_K20170331105440.mpg, 0, result is file not found, LB[125.147.128.5, 125.147.128.5], ClientIP[100.66.14.89]
				strs2 := strings.FieldsFunc(logLine, sepFunc)
				sid := strs2[2]
				gmap[sid] = struct{}{}
			} else if strings.Contains(line, "OnDescribeSemiSetupResponse") {
				// OnDescribeSemiSetupResponse, LB[125.147.128.5, 125.147.128.5], StreamID[2e4ce625-ce3d-40cd-a9a8-679e929ab19d], UUSessionID[29da86f8-c09c-4ef6-a7fc-fd0cd6b7bd2e], ClientIP[100.66.55.48], AssetID[M33H306XSGL1500001_K20170403205934.mpg], bandwidth[0], ServerID[], result[result is file not found]
				idx := strings.Index(logLine, "UUSessionID")
				strs2 := strings.FieldsFunc(logLine[idx:], sepFunc)
				sid := strs2[1]
				gmap[sid] = struct{}{}
			}
		} else if strings.Contains(line, "Successfully New Setup Session") ||
			strings.Contains(line, "Successfully New SemiSetup Session") {
			strs := strings.SplitN(line, ",", 8)
			si := &glblog.SessionInfo{}
			strStarted := strs[2] + " " + strs[3]
			si.Started, _ = time.ParseInLocation(layout, strStarted, loc)

			logLine := strings.Trim(strs[7], `"`)

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
			}

			if isCenter {
				si.IsCenter = true
			} else if _, ok := gmap[si.SID]; ok {
				si.IsCenter = true
			}
			smap[si.SID] = si
		} else if strings.Contains(line, "OnTeardownNotification") {
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

					if fi, ok := fmap[si.Filename]; ok {
						if si.Filesize == 0 && fi.filesize != 0 {
							si.Filesize = fi.filesize
						}
						if si.Bandwidth == 0 && fi.bitrate != 0 {
							si.Bandwidth = fi.bitrate
						}
					}

					if len(logs) > 0 {
						if si.Filesize == 0 && logs[0].Filesize != 0 {
							si.Filesize = logs[0].Filesize
						}
						if si.Bandwidth == 0 && logs[0].Bitrate != 0 {
							si.Bandwidth = logs[0].Bitrate
						}
					}

					fmap[si.Filename] = &fileInfo{si.Bandwidth, si.Filesize}

					if si.Bandwidth == 0 {
						if avgBitrate != 0 {
							si.Bandwidth = avgBitrate
						} else {
							si.Bandwidth = calcAvgBitrate(fmap)
						}
					}

					if assetOnly == false {
						var buf bytes.Buffer
						enc := gob.NewEncoder(&buf)
						err = enc.Encode(si)
						if err != nil {
							log.Fatal(err)
						}
						batch.Put([]byte(si.Started.Format(layout)+si.SID), buf.Bytes())
					}
				}
			}
		}
	}

	if assetOnly == false {
		err = db.Write(batch, nil)
		if err != nil {
			log.Fatal(err)
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
