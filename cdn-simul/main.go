package main

import (
	"bytes"
	"container/heap"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/glblog"
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

func strToTime(str string) time.Time {
	loc, _ := time.LoadLocation("Local")
	t, _ := time.ParseInLocation(layout, str, loc)
	return t
}

func timeToStr(t time.Time) string {
	return t.Format(layout)
}

func readEvent(iter iterator.Iterator) *glblog.SessionInfo {
	if !iter.Next() {
		return nil
	}
	reader := bytes.NewReader(iter.Value())
	dec := gob.NewDecoder(reader)
	var e glblog.SessionInfo
	err := dec.Decode(&e)
	if err != nil {
		log.Fatal(err)
	}
	return &e
}

var eventIdx int

func readEventTest() *glblog.SessionInfo {
	eventIdx++
	evtList := []*glblog.SessionInfo{
		&glblog.SessionInfo{
			SID:       "54a9ced8-9ae6-4fb9-8dad-d8187af1db00",
			Started:   strToTime("2017-04-29 08:16:30.108"),
			Ended:     strToTime("2017-04-29 08:17:05.189"),
			Filename:  "MXCF6UGASGL1000041_K20150604122403.mpg",
			Bandwidth: 6443027,
			Offset:    376,
		},

		&glblog.SessionInfo{
			SID:       "89de68d5-e00f-4365-b57b-c318d2274b8c",
			Started:   strToTime("2017-04-29 08:16:31.148"),
			Ended:     strToTime("2017-04-29 08:19:40.516"),
			Filename:  "MT6EBI1TSGL0800002_K20121009164502.mpg",
			Bandwidth: 2881222,
			Offset:    53768,
		},
		&glblog.SessionInfo{
			SID:       "051bfc9e-b888-4625-9b6b-ef2505e48b51",
			Started:   strToTime("2017-04-29 08:16:31.254"),
			Ended:     strToTime("2017-04-29 08:18:13.469"),
			Filename:  "MT6F700QSGL1000041_K20150714151153.mpg",
			Bandwidth: 6443012,
			Offset:    376,
		},

		&glblog.SessionInfo{
			SID:       "c932bf70-0896-4e6a-b83e-9453bebc7087",
			Started:   strToTime("2017-04-29 08:16:31.923"),
			Ended:     strToTime("2017-04-29 08:25:11.670"),
			Filename:  "MXQG40YVSGL1500001_K20160531224338.mpg",
			Bandwidth: 6462167,
			Offset:    376,
		},
		&glblog.SessionInfo{
			SID:       "519ae106-83aa-41ee-8cdc-b92debf38446",
			Started:   strToTime("2017-04-29 08:16:32.116"),
			Ended:     strToTime("2017-04-29 08:20:11.905"),
			Filename:  "MCLH302CSGL1500001_K20170313201227.mpg",
			Bandwidth: 6459963,
			Offset:    376,
		},

		&glblog.SessionInfo{
			SID:       "73e9381e-3924-4d0e-9b54-e875c5fd049b",
			Started:   strToTime("2017-04-29 08:16:33.768"),
			Ended:     strToTime("2017-04-29 08:19:36.559"),
			Filename:  "MT6EBI1DSGL0800001_K20121008212437.mpg",
			Bandwidth: 2881119,
			Offset:    53768,
		},
		&glblog.SessionInfo{
			SID:       "0e192ee5-3870-4cf1-9247-a9563b5ac981",
			Started:   strToTime("2017-04-29 08:16:35.786"),
			Ended:     strToTime("2017-04-29 08:17:55.046"),
			Filename:  "MIAG12OESGL1500001_K20160202192944.mpg",
			Bandwidth: 6458352,
			Offset:    376,
		},

		&glblog.SessionInfo{
			SID:       "5bf9ace7-0e07-4da7-82da-d0dd877abbb1",
			Started:   strToTime("2017-04-29 08:16:37.499"),
			Ended:     strToTime("2017-04-29 08:16:39.015"),
			Filename:  "MIAG12N5SGL1500001_K20160202192041.mpg",
			Bandwidth: 10552998,
			Offset:    0,
		},
		&glblog.SessionInfo{
			SID:       "45cc15fc-bf17-4fe2-b37c-eeb0a5c73d44",
			Started:   strToTime("2017-04-29 08:16:39.012"),
			Ended:     strToTime("2017-04-29 08:18:02.096"),
			Filename:  "MIAG12N5SGL1500001_K20160202192041.mpg",
			Bandwidth: 6459282,
			Offset:    376,
		},
		&glblog.SessionInfo{
			SID:       "f7c167dc-2367-424d-99f4-966f43faa9d8",
			Started:   strToTime("2017-04-29 08:16:39.885"),
			Ended:     strToTime("2017-04-29 09:17:10.466"),
			Filename:  "M02G8032SGL1500001_K20160816003015.mpg",
			Bandwidth: 6600033,
			Offset:    376,
		},
	}
	if eventIdx > 10 {
		return nil
	}
	return evtList[eventIdx-1]
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

func updateStat(ti time.Time, cfg *data.Config, st *status.Status, gst *stat) {
	if st.Origin.Bps > gst.maxOriginBps {
		gst.maxOriginBps = st.Origin.Bps
	}

	if gst.nextHitResetIdx == -1 && len(cfg.HitResetTimes) > 0 {
		gst.nextHitResetIdx = 0
	}
	hitReset := false
	if gst.nextHitResetIdx >= 0 && time.Time(cfg.HitResetTimes[gst.nextHitResetIdx]).Before(ti) {
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

func writeToDB(ti time.Time, cfg *data.Config, st *status.Status, gst *stat) {
	str := ""
	t := ti.UnixNano()
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

func logStatus(ti time.Time, cfg *data.Config, st *status.Status, gst *stat, writeDB bool) {
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
		writeToDB(ti, cfg, st, gst)
	}
}

type eventHeap []endEvent

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return h[i].time.Before(h[j].time) }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x interface{}) {
	*h = append(*h, x.(endEvent))
}

func (h *eventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type endEventType int

const (
	sessionEnd endEventType = iota
	chunkEnd
)

type endEvent struct {
	endType        endEventType
	time           time.Time
	sid            string
	filename       string
	bps            int
	index          int
	duration       time.Duration
	sessionEndTime time.Time
}

func (e endEvent) String() string {
	return fmt.Sprintf("%v %s %s %s %8d %4d %v %s",
		e.endType, e.time.Format(layout), e.sid, e.filename, e.bps, e.index, e.duration, e.sessionEndTime.Format(layout))
}

func processEventsUntil(ti time.Time, events *eventHeap, lb *lb.LB, cfg *data.Config, gst *stat, logPeriod time.Duration, writeDB bool) {
	for events.Len() > 0 {
		e := heap.Pop(events)
		endEv := e.(endEvent)

		if logPeriod == 0 {
			log.Printf("%s\n", endEv)
		}

		if endEv.time.After(ti) {
			heap.Push(events, endEv)
			return
		}

		var st *status.Status
		var err error
		diffLastChunkTandSessionEndT := time.Millisecond
		if endEv.endType == chunkEnd {
			evt := data.ChunkEvent{
				Time:      endEv.time,
				SessionID: endEv.sid,
				FileName:  endEv.filename,
				Bps:       int64(endEv.bps),
				Index:     int64(endEv.index),
				ChunkSize: chunkSize,
			}
			st, err = lb.EndChunk(evt)
			if err != nil {
				log.Fatalf("failed to process end-chunk-event, %v", err)
			}
			updateStat(evt.Time, cfg, st, gst)
			if logPeriod == 0 {
				log.Printf("chunk end: %s\n", evt)
				logStatus(evt.Time, cfg, st, gst, writeDB)
			}
			if endEv.sessionEndTime.Sub(endEv.time) == diffLastChunkTandSessionEndT {
				continue
			}

			evt.Index++
			st, err = lb.StartChunk(evt)
			if err != nil {
				log.Fatalf("failed to process start-chunk-event, %v", err)
			}
			updateStat(evt.Time, cfg, st, gst)
			if logPeriod == 0 {
				log.Printf("chunk start: %s\n", evt)
				logStatus(evt.Time, cfg, st, gst, writeDB)
			}

			nextEndT := endEv.time.Add(endEv.duration)
			if nextEndT.Before(endEv.sessionEndTime) {
				endEv.time = nextEndT
			} else {
				endEv.time = endEv.sessionEndTime.Add(-diffLastChunkTandSessionEndT)
			}
			endEv.index++
			heap.Push(events, endEv)
		} else {
			evt := data.SessionEvent{
				Time:      endEv.time,
				SessionID: endEv.sid,
				FileName:  endEv.filename,
				Bps:       int64(endEv.bps),
			}
			st, err = lb.EndSession(evt)
			if err != nil {
				log.Fatalf("failed to process end-sesison-event, %v", err)
			}
			updateStat(evt.Time, cfg, st, gst)
			if logPeriod == 0 {
				log.Printf("session end: %s\n", evt)
				logStatus(evt.Time, cfg, st, gst, writeDB)
			}
		}
	}
}

var dbAddr, dbUser, dbPass, dbName string

func main() {
	var cfgFile, dbFile, cpuprofile, memprofile, lp string
	var readEventCount int
	var writeDB bool

	flag.StringVar(&cfgFile, "cfg", "cdn-simul.json", "config file")
	flag.StringVar(&dbFile, "db", "chunk.db", "event db")
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

	endEvents := &eventHeap{}
	heap.Init(endEvents)

	db, err := leveldb.OpenFile(dbFile, nil)
	if err != nil {
		log.Fatalf("failed to open db, %v", err)
	}
	iter := db.NewIterator(nil, nil)

	now := time.Now()
	var nextLogT time.Time
	var procT time.Time
	evtCount := int64(0)
	gStat := &stat{vods: make(map[string]vodStat), nextHitResetIdx: -1}
	for {
		evtCount++
		if readEventCount != 0 && int(evtCount) > readEventCount {
			break
		}
		ev := readEvent(iter)
		//ev := readEventTest()
		if ev == nil {
			processEventsUntil(strToTime("9999-12-31 00:00:00.000"), endEvents, lb, &cfg, gStat, logPeriod, writeDB)
			break
		}
		if evtCount == 1 {
			nextLogT = ev.Started
		}
		procT = ev.Started

		processEventsUntil(procT, endEvents, lb, &cfg, gStat, logPeriod, writeDB)

		if logPeriod == 0 {
			log.Printf("session event: %s\n", ev)
		}

		var st *status.Status
		var err error
		sEvt := data.SessionEvent{
			Time:      ev.Started,
			SessionID: ev.SID,
			FileName:  ev.Filename,
			Bps:       int64(ev.Bandwidth),
		}
		st, err = lb.StartSession(sEvt)
		if err != nil {
			log.Fatalf("failed to process start-session-event, %v", err)
		}
		updateStat(ev.Started, &cfg, st, gStat)
		if logPeriod == 0 {
			log.Printf("session start: %s\n", sEvt)
			logStatus(ev.Started, &cfg, st, gStat, writeDB)
		} else if ev.Started.After(nextLogT) {
			logStatus(ev.Started, &cfg, st, gStat, writeDB)
			for {
				nextLogT = nextLogT.Add(logPeriod)
				if nextLogT.After(ev.Started) {
					break
				}
			}
		}

		idx := int(ev.Offset / chunkSize)
		du := time.Duration(float64(8*chunkSize)/float64(ev.Bandwidth)*1000) * time.Millisecond
		cEvt := data.ChunkEvent{
			Time:      ev.Started,
			SessionID: ev.SID,
			FileName:  ev.Filename,
			Bps:       int64(ev.Bandwidth),
			Index:     int64(idx),
			ChunkSize: chunkSize,
		}
		st, err = lb.StartChunk(cEvt)
		if err != nil {
			log.Fatalf("failed to process start-chunk-event, %v", err)
		}
		updateStat(ev.Started, &cfg, st, gStat)
		if logPeriod == 0 {
			log.Printf("chunk start: %s\n", cEvt)
			logStatus(ev.Started, &cfg, st, gStat, writeDB)
		}

		ecEv := endEvent{
			time:           ev.Started.Add(du),
			endType:        chunkEnd,
			sid:            ev.SID,
			filename:       ev.Filename,
			bps:            ev.Bandwidth,
			index:          idx,
			duration:       du,
			sessionEndTime: ev.Ended,
		}
		if ecEv.time.After(ev.Ended) {
			ecEv.time = ev.Ended.Add(-time.Millisecond)
		}
		heap.Push(endEvents, ecEv)

		esEv := endEvent{
			time:           ev.Ended,
			endType:        sessionEnd,
			sid:            ev.SID,
			filename:       ev.Filename,
			bps:            ev.Bandwidth,
			index:          idx,
			duration:       du,
			sessionEndTime: ev.Ended,
		}
		heap.Push(endEvents, esEv)
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
