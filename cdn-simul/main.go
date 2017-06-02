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
	"net"
	"net/http"
	"os"
	"runtime/pprof"
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
	nextHitResetIdx     int
	maxOriginBps        int64
	vods                map[string]vodStat
	hitResetWhenAllFull bool
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

		defer func() {
			gst.nextHitResetIdx++
			if gst.nextHitResetIdx == len(cfg.HitResetTimes) {
				gst.nextHitResetIdx = -2
			}
		}()
	}
	if st.AllCacheFull && !gst.hitResetWhenAllFull {
		hitReset = true
		log.Println("all cache full, hit rate reset!!!")
		gst.hitResetWhenAllFull = true
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
}

func writeToDB(cfg *data.Config, st *status.Status, gst *stat, ev *event) {
	str := ""
	t := ev.EventTime.UnixNano()
	for _, v := range st.Caches {
		vcfg := findConfig(cfg, v.VODKey)
		vod := st.Vods[vod.Key(v.VODKey)]
		str += fmt.Sprintf("cache,vod=%s hit=%d,miss=%d,originbps=%d,disk=%d,disklimit=%d %d\n",
			v.VODKey, v.CacheHitCount, v.CacheMissCount, v.OriginBps, v.CurSize, vcfg.StorageSize, t)
		str += fmt.Sprintf("vod,vod=%s bps=%d,bpslimit=%d,session=%d,sessionlimit=%d %d\n",
			v.VODKey, vod.CurBps, vcfg.LimitBps, vod.CurSessionCount, vcfg.LimitSession, t)
	}

	reqBody := bytes.NewBufferString(str)
	cl := &http.Client{
		Timeout: 3 * time.Second,
		// http.DefaultTransport + (DisableKeepAlives: true)
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			DisableKeepAlives:     true,
		},
	}

	req, err := http.NewRequest("POST", "http://"+dbAddr+"/write?db="+dbName, reqBody)
	if err != nil {
		log.Printf("failed to creat request, %v\n", err)
		return
	}
	resp, err := cl.Do(req)
	if err != nil {
		log.Printf("failed to post request, %v\n", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		b, _ := ioutil.ReadAll(resp.Body)
		log.Printf("failed to post request, status:%s, body:%s\n", resp.Status, string(b))
		return
	}
}

func logStatus(cfg *data.Config, st *status.Status, gst *stat, ev *event, writeDB bool) {
	str := ""
	totalHit := int64(0)
	totalMiss := int64(0)

	hitRateFn := func(hit, miss int64) int {
		if hit == 0 {
			return 0
		}
		return int(float64(hit) * 100 / float64(hit+miss))
	}
	// cfg의 VOD 순으로 logging
	for _, cc := range cfg.VODs {
		v := st.Vods[vod.Key(cc.VodID)]
		cache := findCacheStatus(st, v.VODKey)
		vc := findConfig(cfg, v.VODKey)

		hit := cache.CacheHitCount - gst.vods[v.VODKey].hitCountWhenReset
		miss := cache.CacheMissCount - gst.vods[v.VODKey].missCountWhenReset
		totalHit += hit
		totalMiss += miss
		str += fmt.Sprintf("%s [%15s session(%4v/%4v/%3v%%/max:%3v%%) bps(%7v/%7v/%3v%%/max:%3v%%) disk(%8v/%8v/%3v%%) hit(%5v/%5v: %3v %%) origin(%6v)]\n",
			st.Time.Format(layout),
			v.VODKey, v.CurSessionCount, vc.LimitSession, int(float64(v.CurSessionCount)*100/float64(vc.LimitSession)), gst.vods[v.VODKey].maxSessionPercent,
			humanize.Bytes(uint64(v.CurBps)), humanize.Bytes(uint64(vc.LimitBps)), int(float64(v.CurBps)*100/float64(vc.LimitBps)), gst.vods[v.VODKey].maxBpsPercent,
			humanize.IBytes(uint64(cache.CurSize)), humanize.IBytes(uint64(vc.StorageSize)), int(float64(cache.CurSize)*100/float64(vc.StorageSize)),
			hit, hit+miss, hitRateFn(hit, miss),
			humanize.Bytes(uint64(cache.OriginBps)))
	}

	str = fmt.Sprintf("\n%s all-full:%v originBps(cur:%4v/max:%4v) hit(%4v/%4v: %3v %%)\n",
		st.Time.Format(layout),
		st.AllCacheFull, humanize.Bytes(uint64(st.Origin.Bps)), humanize.Bytes(uint64(gst.maxOriginBps)),
		totalHit, totalHit+totalMiss, hitRateFn(totalHit, totalMiss)) + str
	fmt.Println(str)

	if dbAddr != "" {
		writeToDB(cfg, st, gst, ev)
	}
}

var dbAddr, dbUser, dbPass, dbName string

func main() {
	var cfgFile, dbFile, fileCsvFile, cpuprofile, memprofile, lp string
	var readEventCount int
	var writeDB bool

	flag.StringVar(&cfgFile, "cfg", "cdn-simul.json", "config file")
	flag.StringVar(&dbFile, "db", "chunk.db", "event db")
	flag.StringVar(&fileCsvFile, "file-csv", "files.csv", "event db")
	flag.IntVar(&readEventCount, "event-count", 0, "event count to process. if 0, process all event")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write cpu profile")
	flag.StringVar(&memprofile, "memprofile", "", "write memory profile")
	flag.StringVar(&lp, "log-period", "0s", "status logging period (second). if 0, print log after every event")
	flag.StringVar(&dbAddr, "db-addr", "", "DB address. if empty, not use DB. ex: localhost:8086")
	flag.StringVar(&dbUser, "db-user", "", "DB username")
	flag.StringVar(&dbPass, "db-pass", "", "DB password")
	flag.StringVar(&dbName, "db-name", "mydb", "database name")

	flag.Parse()

	logPeriod, err := time.ParseDuration(lp)
	if err != nil {
		log.Fatal(err)
	}

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
	var nextLogT time.Time
	evtCount := int64(0)
	gStat := &stat{vods: make(map[string]vodStat), nextHitResetIdx: -1}
	for {
		evtCount++
		if readEventCount != 0 && int(evtCount) > readEventCount {
			break
		}
		ev := readEvent(iter)
		if ev == nil {
			break
		}
		if logPeriod == 0 {
			log.Printf("%s\n", ev)
		}
		if evtCount == 1 {
			nextLogT = ev.EventTime
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
			if logPeriod == 0 || ev.EventTime.After(nextLogT) {
				logStatus(&cfg, st, gStat, ev, writeDB)
				for {
					nextLogT = nextLogT.Add(logPeriod)
					if nextLogT.After(ev.EventTime) {
						break
					}
				}
			}
		} else {
			if logPeriod == 0 {
				logStatus(&cfg, st, gStat, ev, writeDB)
			}
		}
	}

	log.Printf("completed. elapsed:%v\n", time.Since(now))

	if memprofile != "" {
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatalf("failed to start memory profile, %v", err)
		}
		pprof.WriteHeapProfile(f)
		defer f.Close()
	}
}
