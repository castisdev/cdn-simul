package simul

import (
	"container/heap"
	"fmt"
	"log"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb"
	"github.com/castisdev/cdn-simul/status"
)

const chunkSize int64 = 2000000 //2 * 1024 * 1024

// Options :
type Options struct {
	MaxReadEventCount int
	InfluxDBAddr      string
	InfluxDBName      string
	InfluxDBUser      string
	InfluxDBPass      string
	StatusWritePeriod time.Duration
	BypassFile        string
	FirstBypass       bool
	FBPeriod          time.Duration
	SimulID           string
	StartTime         time.Time
}

var layout = "2006-01-02 15:04:05.000"

// StrToTime :
func StrToTime(str string) time.Time {
	loc, _ := time.LoadLocation("Local")
	t, _ := time.ParseInLocation(layout, str, loc)
	return t
}

// TimeToStr :
func TimeToStr(t time.Time) string {
	return t.Format(layout)
}

// FindCacheStatus :
func FindCacheStatus(s *status.Status, k string) *status.CacheStatus {
	for _, v := range s.Caches {
		if v.VODKey == k {
			return v
		}
	}
	return nil
}

// FindConfig :
func FindConfig(c *data.Config, k string) data.VODConfig {
	for _, v := range c.VODs {
		if v.VodID == k {
			return v
		}
	}
	return data.VODConfig{}
}

type eventHeap []*endEvent

func (h eventHeap) Len() int           { return len(h) }
func (h eventHeap) Less(i, j int) bool { return h[i].time.Before(h[j].time) }
func (h eventHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *eventHeap) Push(x interface{}) {
	*h = append(*h, x.(*endEvent))
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
	intFilename    int
	bps            int
	index          int
	duration       time.Duration
	sessionEndTime time.Time
	bypass         bool
	useOrigin      bool
	isCenter       bool
}

func (e endEvent) String() string {
	return fmt.Sprintf("%v %s %s %s %8d %4d %v %s %v",
		e.endType, e.time.Format(layout), e.sid, e.filename, e.bps, e.index, e.duration, e.sessionEndTime.Format(layout), e.bypass)
}

type firstBypassChecker struct {
	firstHitFile    map[int]struct{}
	moreHitFile     map[int]struct{}
	updatedHitFileT time.Time
	updateHitPeriod time.Duration
}

func (s *firstBypassChecker) updateHitFile(file int, t time.Time) {
	if s.updatedHitFileT.IsZero() {
		s.updatedHitFileT = t
	} else if t.Sub(s.updatedHitFileT) >= s.updateHitPeriod {
		for k := range s.firstHitFile {
			delete(s.firstHitFile, k)
		}
		for k := range s.moreHitFile {
			delete(s.moreHitFile, k)
		}
		s.updatedHitFileT = t
	}
	_, firstOk := s.firstHitFile[file]
	_, moreOk := s.moreHitFile[file]
	var empty struct{}
	if !firstOk && !moreOk {
		s.firstHitFile[file] = empty
	} else if firstOk {
		delete(s.firstHitFile, file)
		s.moreHitFile[file] = empty
	}
}

func (s *firstBypassChecker) isBypass(file int) bool {
	_, ok := s.firstHitFile[file]
	return ok
}

// Simulator :
type Simulator struct {
	cfg            data.Config
	opt            Options
	reader         EventReader
	writer         StatusWriter
	lb             lb.LoadBalancer
	internalEvents *eventHeap
	bypassMap      map[string]interface{}
	filenameMap    map[string]int
	filenameSeed   int
	firstBypass    *firstBypassChecker
	fileInfos      *data.FileInfos
	startT         time.Time
}

// NewSimulator :
func NewSimulator(cfg data.Config, opt Options, lb lb.LoadBalancer, r EventReader, w StatusWriter, fi *data.FileInfos, bypass []string) *Simulator {
	ie := &eventHeap{}
	heap.Init(ie)

	si := &Simulator{
		cfg:            cfg,
		opt:            opt,
		reader:         r,
		writer:         w,
		lb:             lb,
		internalEvents: ie,
		bypassMap:      make(map[string]interface{}),
		filenameMap:    make(map[string]int),
		firstBypass: &firstBypassChecker{
			firstHitFile:    make(map[int]struct{}),
			moreHitFile:     make(map[int]struct{}),
			updateHitPeriod: opt.FBPeriod,
		},
		fileInfos: fi,
		startT:    opt.StartTime,
	}
	for _, v := range bypass {
		si.bypassMap[v] = nil
	}

	if si.startT.IsZero() == false {
		fmt.Printf("events (started time < %v) will be ignored\n", TimeToStr(si.startT))
	}
	return si
}

func (s *Simulator) getFilename(filename string) int {
	if s.fileInfos != nil {
		return s.fileInfos.IntName(filename)
	}

	if value, ok := s.filenameMap[filename]; ok {
		return value
	}

	s.filenameSeed++
	s.filenameMap[filename] = s.filenameSeed
	return s.filenameSeed
}

// Run :
func (s *Simulator) Run() {
	var nextLogT time.Time
	var procT time.Time
	evtCount := int64(0)
	for {
		evtCount++
		if s.opt.MaxReadEventCount != 0 && int(evtCount) > s.opt.MaxReadEventCount {
			break
		}
		ev := s.reader.ReadEvent()
		if ev == nil {
			s.processEventsUntil(StrToTime("9999-12-31 00:00:00.000"), s.internalEvents, s.lb)
			break
		}
		if evtCount == 1 {
			nextLogT = ev.Started
		}
		if s.fileInfos != nil && s.fileInfos.Exists(ev.Filename) == false {
			sz := ev.Filesize
			if sz == 0 {
				sz = 2 * 1024 * 1024 * 1024
			}
			s.fileInfos.AddOne(ev.Filename, sz, StrToTime("2017-01-01 00:00:00.000"))
		}

		if s.startT.IsZero() == false && s.startT.After(ev.Started) {
			continue
		}
		procT = ev.Started

		s.processEventsUntil(procT, s.internalEvents, s.lb)

		if s.opt.StatusWritePeriod == 0 {
			log.Printf("session event: %s\n", ev)
		}

		fn := s.getFilename(ev.Filename)
		if s.opt.FirstBypass {
			s.firstBypass.updateHitFile(fn, procT)
		}

		var err error
		sEvt := data.SessionEvent{
			Time:        ev.Started,
			SessionID:   ev.SID,
			FileName:    ev.Filename,
			IntFileName: fn,
			FileSize:    ev.Filesize,
			Bps:         int64(ev.Bandwidth),
			Duration:    ev.Ended.Sub(ev.Started),
		}
		err = s.lb.StartSession(&sEvt)
		if err != nil {
			log.Fatalf("failed to process start-session-event, %v", err)
		}
		if s.opt.StatusWritePeriod == 0 {
			log.Printf("session start: %s\n", sEvt)
			st := s.lb.Status(sEvt.Time)
			s.writeStatus(ev.Started, *st, s.cfg, s.opt)
		} else if ev.Started.After(nextLogT) {
			st := s.lb.Status(sEvt.Time)
			s.writeStatus(ev.Started, *st, s.cfg, s.opt)
			for {
				nextLogT = nextLogT.Add(s.opt.StatusWritePeriod)
				if nextLogT.After(ev.Started) {
					break
				}
			}
		}

		idx := int(ev.Offset / chunkSize)
		du := time.Duration(float64(8*chunkSize)/float64(ev.Bandwidth)*1000) * time.Millisecond
		_, bypass := s.bypassMap[ev.Filename]
		if s.opt.FirstBypass {
			bypass = bypass || s.firstBypass.isBypass(fn)
		}
		cEvt := data.ChunkEvent{
			Time:        ev.Started,
			SessionID:   ev.SID,
			FileName:    ev.Filename,
			IntFileName: fn,
			FileSize:    ev.Filesize,
			Bps:         int64(ev.Bandwidth),
			Index:       int64(idx),
			ChunkSize:   chunkSize,
			Bypass:      bypass,
			IsCenter:    ev.IsCenter,
		}
		var useOrigin bool
		useOrigin, err = s.lb.StartChunk(&cEvt)
		if err != nil {
			log.Fatalf("failed to process start-chunk-event, %v", err)
		}
		if s.opt.StatusWritePeriod == 0 {
			log.Printf("chunk start: %s\n", cEvt)
			st := s.lb.Status(cEvt.Time)
			s.writeStatus(ev.Started, *st, s.cfg, s.opt)
		}

		ecEv := endEvent{
			time:           ev.Started.Add(du),
			endType:        chunkEnd,
			sid:            ev.SID,
			filename:       ev.Filename,
			intFilename:    fn,
			bps:            ev.Bandwidth,
			index:          idx,
			duration:       du,
			sessionEndTime: ev.Ended,
			bypass:         bypass,
			useOrigin:      useOrigin,
			isCenter:       ev.IsCenter,
		}
		if ecEv.time.Sub(ev.Ended) >= 0 {
			ecEv.time = ev.Ended.Add(-time.Millisecond)
		}
		heap.Push(s.internalEvents, &ecEv)

		esEv := endEvent{
			time:           ev.Ended,
			endType:        sessionEnd,
			sid:            ev.SID,
			filename:       ev.Filename,
			intFilename:    fn,
			bps:            ev.Bandwidth,
			index:          idx,
			duration:       ev.Ended.Sub(ev.Started),
			sessionEndTime: ev.Ended,
		}
		heap.Push(s.internalEvents, &esEv)
	}
}
func (s *Simulator) writeStatus(ti time.Time, st status.Status, cfg data.Config, opt Options) {
	if s.writer != nil {
		s.writer.WriteStatus(ti, st, cfg, opt)
	}
}

func (s *Simulator) processEventsUntil(ti time.Time, events *eventHeap, lb lb.LoadBalancer) {
	for events.Len() > 0 {
		e := heap.Pop(events)
		endEv := e.(*endEvent)

		if endEv.time.After(ti) {
			heap.Push(events, endEv)
			return
		}

		var err error
		diffLastChunkTandSessionEndT := time.Millisecond
		if endEv.endType == chunkEnd {
			evt := data.ChunkEvent{
				Time:        endEv.time,
				SessionID:   endEv.sid,
				FileName:    endEv.filename,
				IntFileName: endEv.intFilename,
				Bps:         int64(endEv.bps),
				Index:       int64(endEv.index),
				ChunkSize:   chunkSize,
				Bypass:      endEv.bypass,
				IsCenter:    endEv.isCenter,
			}
			err = lb.EndChunk(&evt, endEv.useOrigin)
			if err != nil {
				log.Fatalf("failed to process end-chunk-event, %v", err)
			}
			if s.opt.StatusWritePeriod == 0 {
				log.Printf("chunk end: %s\n", evt)
				st := lb.Status(evt.Time)
				s.writeStatus(evt.Time, *st, s.cfg, s.opt)
			}
			if endEv.sessionEndTime.Sub(endEv.time) == diffLastChunkTandSessionEndT {
				continue
			}

			evt.Index++
			endEv.useOrigin, err = lb.StartChunk(&evt)
			if err != nil {
				log.Fatalf("failed to process start-chunk-event, %v", err)
			}
			if s.opt.StatusWritePeriod == 0 {
				log.Printf("chunk start: %s\n", evt)
				st := lb.Status(evt.Time)
				s.writeStatus(evt.Time, *st, s.cfg, s.opt)
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
				Time:        endEv.time,
				SessionID:   endEv.sid,
				FileName:    endEv.filename,
				IntFileName: endEv.intFilename,
				Bps:         int64(endEv.bps),
				Duration:    endEv.duration,
			}
			err = lb.EndSession(&evt)
			if err != nil {
				log.Fatalf("failed to process end-sesison-event, %v", err)
			}
			if s.opt.StatusWritePeriod == 0 {
				log.Printf("session end: %s\n", evt)
				st := lb.Status(evt.Time)
				s.writeStatus(evt.Time, *st, s.cfg, s.opt)
			}
		}
	}
}

// LBOption :
type LBOption struct {
	Cfg                 data.Config
	LBType              string
	HotListUpdatePeriod time.Duration
	HotRankLimit        int
	StatDuration        time.Duration
	StatDurationForDel  time.Duration
	ShiftPeriod         time.Duration
	PushPeriod          time.Duration
	PushDelayN          int
	DawnPushN           int
	Fileinfos           *data.FileInfos
	InitContents        []string
	DeliverEvent        []*data.DeliverEvent
	PurgeEvent          []*data.PurgeEvent
	UseSessionDuration  bool
	UseDeleteLru        bool
	UseFileSize         bool
	UseTimeWeight       bool
}

// NewLoadBalancer :
func NewLoadBalancer(opt LBOption) (lb.LoadBalancer, error) {
	switch opt.LBType {
	case "legacy":
		return lb.NewLegacyLB(opt.Cfg, &lb.SameHashingWeight{})
	case "filebase":
		st := lb.NewStorage(opt.StatDuration, opt.StatDurationForDel, opt.ShiftPeriod, opt.PushPeriod, opt.PushDelayN, opt.DawnPushN,
			opt.Cfg.VODs[0].StorageSize, opt.Fileinfos, opt.InitContents, opt.DeliverEvent, opt.PurgeEvent,
			opt.UseSessionDuration, opt.UseDeleteLru, opt.UseFileSize, opt.UseTimeWeight)
		return lb.NewFilebaseLB(opt.Cfg, lb.NewFileBase(st))
	}
	s := NewVODSelector(opt.LBType, opt.HotListUpdatePeriod, opt.HotRankLimit)
	return lb.New(opt.Cfg, s)
}

// NewVODSelector :
func NewVODSelector(algorithm string, hotListUpdatePeriod time.Duration, hotRankLimit int) lb.VODSelector {
	switch algorithm {
	case "weight-storage-bps":
		return &lb.WeightStorageBps{}
	case "dup2":
		return &lb.SameWeightDup2{}
	case "weight-storage":
		return &lb.WeightStorage{}
	case "high-low":
		return lb.NewHighLowGroup(hotListUpdatePeriod, hotRankLimit)
	}
	return &lb.SameHashingWeight{}
}
