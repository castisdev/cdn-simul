package main

import (
	"bytes"
	"encoding/csv"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn-simul/status"
	"github.com/castisdev/cdn/profile"
	"github.com/dustin/go-humanize"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

const chunkSize int64 = 2 * 1024 * 1024

var layout = "2006-01-02 15:04:05.000"

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

func strToTime(str string) time.Time {
	loc, _ := time.LoadLocation("Local")
	t, _ := time.ParseInLocation(layout, str, loc)
	return t
}

func timeToStr(t time.Time) string {
	return t.Format(layout)
}

func readEvent(iter iterator.Iterator) *event {
	if !iter.Next() {
		return nil
	}
	reader := bytes.NewReader(iter.Value())
	dec := gob.NewDecoder(reader)
	var e event
	err := dec.Decode(&e)
	if err != nil {
		log.Fatal(err)
	}
	return &e
}

type stat struct {
	nextHitResetIdx int
	maxOriginBps    int64
	vods            map[string]vodStat
}

type vodStat struct {
	maxSessionPercent  int
	maxBpsPercent      int
	missCountWhenReset int64
	hitCountWhenReset  int64
}

func findCacheStatus(s *status.Status, k string) *status.CacheStatus {
	for _, v := range s.Caches {
		if v.VODKey == k {
			return v
		}
	}
	return nil
}

func findConfig(c *data.Config, k string) data.VODConfig {
	for _, v := range c.VODs {
		if v.VodID == k {
			return v
		}
	}
	return data.VODConfig{}
}

func updateStat(cfg *data.Config, evt *event, st *status.Status, gst *stat) {
	if st.Origin.Bps > gst.maxOriginBps {
		gst.maxOriginBps = st.Origin.Bps
	}

	if gst.nextHitResetIdx == -1 && len(cfg.HitResetTimes) > 0 {
		gst.nextHitResetIdx = 0
	}
	hitReset := false
	if gst.nextHitResetIdx >= 0 && time.Time(cfg.HitResetTimes[gst.nextHitResetIdx]).Before(evt.EventTime) {
		hitReset = true
		log.Println("hit rate reset!!!")
	}

	for _, v := range st.Vods {
		vc := findConfig(cfg, v.VODKey)
		cache := findCacheStatus(st, v.VODKey)
		maxBps := int(float64(v.CurBps) * 100 / float64(vc.LimitBps))
		maxSession := int(float64(v.CurSessionCount) * 100 / float64(vc.LimitSession))
		if _, ok := gst.vods[v.VODKey]; !ok {
			gst.vods[v.VODKey] = vodStat{}
		}
		s := gst.vods[v.VODKey]
		if s.maxBpsPercent < maxBps {
			s.maxBpsPercent = maxBps
			gst.vods[v.VODKey] = s
		}
		if s.maxSessionPercent < maxSession {
			s.maxSessionPercent = maxSession
			gst.vods[v.VODKey] = s
		}

		if hitReset {
			s.hitCountWhenReset = cache.CacheHitCount
			s.missCountWhenReset = cache.CacheMissCount
			gst.vods[v.VODKey] = s
		}
	}
	if hitReset {
		gst.nextHitResetIdx++
		if gst.nextHitResetIdx == len(cfg.HitResetTimes) {
			gst.nextHitResetIdx = -2
		}
	}
}

func logStatus(cfg *data.Config, st *status.Status, gst *stat) {
	str := fmt.Sprintf("\n%s all-full:%v originBps(cur:%v/max:%v)\n",
		st.Time.Format(layout), st.AllCacheFull, humanize.Bytes(uint64(st.Origin.Bps)), humanize.Bytes(uint64(gst.maxOriginBps)))

	// cfg의 VOD 순으로 logging
	for _, cc := range cfg.VODs {
		v := st.Vods[vod.Key(cc.VodID)]
		cache := findCacheStatus(st, v.VODKey)
		vc := findConfig(cfg, v.VODKey)

		hit := cache.CacheHitCount - gst.vods[v.VODKey].hitCountWhenReset
		miss := cache.CacheMissCount - gst.vods[v.VODKey].missCountWhenReset
		str += fmt.Sprintf("[%s session(%v/%v/%v%%/max:%v%%) bps(%v/%v/%v%%/max:%v%%) disk(%v/%v/%v%%) hit(%v %%) origin(%v)]\n",
			v.VODKey, v.CurSessionCount, vc.LimitSession, int(float64(v.CurSessionCount)*100/float64(vc.LimitSession)), gst.vods[v.VODKey].maxSessionPercent,
			humanize.Bytes(uint64(v.CurBps)), humanize.Bytes(uint64(vc.LimitBps)), int(float64(v.CurBps)*100/float64(vc.LimitBps)), gst.vods[v.VODKey].maxBpsPercent,
			humanize.IBytes(uint64(cache.CurSize)), humanize.IBytes(uint64(vc.StorageSize)), int(float64(cache.CurSize)*100/float64(vc.StorageSize)),
			int(float64(hit)*100/float64(hit+miss)),
			humanize.Bytes(uint64(cache.OriginBps)))

	}
	log.Println(str)
}

func main() {
	var cfgFile, dbFile, fileCsvFile, cpuprofile string
	var readEventCount, logPeriod int

	flag.StringVar(&cfgFile, "cfg", "cdn-simul.json", "config file")
	flag.StringVar(&dbFile, "db", "chunk.db", "event db")
	flag.StringVar(&fileCsvFile, "file-csv", "files.csv", "event db")
	flag.IntVar(&readEventCount, "event-count", 0, "event count to process. if 0, process all event")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile")
	flag.IntVar(&logPeriod, "log-period", 0, "status logging period (second). if 0, print log after every event")

	flag.Parse()

	if cpuprofile != "" {
		if err := profile.StartCPUProfile(cpuprofile); err != nil {
			log.Fatalf("failed to start cpu profile, %v", err)
		}
		defer profile.StopCPUProfile()
	}

	f, err := os.OpenFile(cfgFile, os.O_RDONLY, 0755)
	if err != nil {
		log.Fatalf("failed to open cfg, %v", err)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatalf("failed to read cfg, %v", err)
	}
	cfg := data.Config{}
	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Fatalf("failed to unmarsharl cfg json, %v", err)
	}

	lb, err := lb.New(cfg)
	if err != nil {
		log.Fatalf("failed to create lb instance, %v", err)
	}

	ffile, err := os.OpenFile(fileCsvFile, os.O_RDONLY, 0755)
	if err != nil {
		log.Fatalf("failed to open files csv, %v", err)
	}
	defer ffile.Close()

	freader := csv.NewReader(ffile)
	bpsMap := make(map[string]int)
	for {
		record, err := freader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("failed to read csv, %v", err)
		}
		bps, err := strconv.Atoi(strings.Trim(record[1], " "))
		if err != nil {
			log.Fatalf("failed to atoi, %v", err)
		}
		bpsMap[record[0]] = bps
	}

	db, err := leveldb.OpenFile(dbFile, nil)
	if err != nil {
		log.Fatalf("failed to open db, %v", err)
	}
	iter := db.NewIterator(nil, nil)

	now := time.Now()
	logT := time.Now()
	evtCount := 0
	gStat := &stat{vods: make(map[string]vodStat), nextHitResetIdx: -1}
	for {
		evtCount++
		if readEventCount != 0 && evtCount > readEventCount {
			break
		}
		ev := readEvent(iter)
		if ev == nil {
			break
		}
		if logPeriod == 0 {
			log.Printf("%s\n", ev)
		}

		var st *status.Status
		var err error
		switch ev.EventType {
		case sessionCreated:
			evt := data.SessionEvent{
				Time:      ev.EventTime,
				SessionID: ev.SID,
				FileName:  ev.Filename,
				Bps:       int64(bpsMap[ev.Filename]),
			}
			st, err = lb.StartSession(evt)
		case sessionClosed:
			evt := data.SessionEvent{
				Time:      ev.EventTime,
				SessionID: ev.SID,
				FileName:  ev.Filename,
				Bps:       int64(bpsMap[ev.Filename]),
			}
			st, err = lb.EndSession(evt)
		case chunkCreated:
			evt := data.ChunkEvent{
				Time:      ev.EventTime.Add(time.Millisecond),
				SessionID: ev.SID,
				FileName:  ev.Filename,
				Bps:       int64(bpsMap[ev.Filename]),
				Index:     int64(ev.Index),
				ChunkSize: chunkSize,
			}
			st, err = lb.StartChunk(evt)
		case chunkClosed:
			evt := data.ChunkEvent{
				Time:      ev.EventTime.Add(-time.Millisecond),
				SessionID: ev.SID,
				FileName:  ev.Filename,
				Bps:       int64(bpsMap[ev.Filename]),
				Index:     int64(ev.Index),
				ChunkSize: chunkSize,
			}
			st, err = lb.EndChunk(evt)
		}
		if err != nil {
			log.Fatalf("failed to event %s, %v", ev, err)
		}
		updateStat(&cfg, ev, st, gStat)
		if ev.EventType == sessionCreated {
			if logPeriod == 0 || time.Since(logT) > (time.Duration(logPeriod)*time.Second) {
				logStatus(&cfg, st, gStat)
				logT = time.Now()
			}
		} else {
			if logPeriod == 0 {
				logStatus(&cfg, st, gStat)
			}
		}
	}

	log.Printf("completed. elapsed:%v\n", time.Since(now))
}
