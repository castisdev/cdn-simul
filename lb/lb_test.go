package lb

import (
	"testing"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn-simul/status"
	"github.com/stretchr/testify/assert"
)

// EventTypeEnum :
type EventTypeEnum int

// Event Types
const (
	StartSessionType EventTypeEnum = iota
	EndSessionType
	StartChunkType
	EndChunkType
)

func TestLB_OneVOD(t *testing.T) {
	assert := assert.New(t)

	v1 := data.VODConfig{VodID: "v1", StorageSize: 100, LimitSession: 2, LimitBps: 100}
	lb, err := New(data.Config{VODs: []data.VODConfig{v1}})
	assert.Nil(err)

	stat := &status.Status{
		Origin: &status.OriginStatus{},
		Vods:   make(map[vod.Key]*status.VODStatus),
		Caches: make(map[vod.Key]*status.CacheStatus),
	}

	tests := []struct {
		name         string
		eventType    EventTypeEnum
		event        interface{}
		expectError  bool
		expectStatus *status.Status
		setupFn      func()
	}{
		{"StartSession sess1", StartSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC),
			SessionID: "sess1",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 30, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID}
		}},

		{"StartChunk a.mpg-0, miss (sess1)", StartChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 1, 0, time.UTC),
			SessionID: "sess1",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
			Index:     0,
			ChunkSize: 20,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 1, 0, time.UTC)
			stat.Origin.Bps = 30
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 30, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheMissCount: 1, OriginBps: 30, CurSize: 20}
		}},

		{"StartSession sess2", StartSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 3, 0, time.UTC),
			SessionID: "sess2",
			FileName:  "b.mpg",
			FileSize:  20,
			Bps:       20,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 3, 0, time.UTC)
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 50, CurSessionCount: 2}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheMissCount: 1, OriginBps: 30, CurSize: 20}
		}},

		{"StartSession sess3, reaches limitSession", StartSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 6, 0, time.UTC),
			SessionID: "sess3",
			FileName:  "c.mpg",
			FileSize:  20,
			Bps:       20,
		}, true, nil, nil},

		{"EndChunk a.mpg-0 (sess1)", EndChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 6, 1, time.UTC),
			SessionID: "sess1",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
			Index:     0,
			ChunkSize: 20,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 6, 1, time.UTC)
			stat.Origin.Bps = 0
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 50, CurSessionCount: 2}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheMissCount: 1, OriginBps: 0, CurSize: 20}
		}},

		{"StartChunk a.mpg-1, miss (sess1)", StartChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 6, 2, time.UTC),
			SessionID: "sess1",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
			Index:     1,
			ChunkSize: 10,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 6, 2, time.UTC)
			stat.Origin.Bps = 30
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 50, CurSessionCount: 2}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheMissCount: 2, OriginBps: 30, CurSize: 30}
		}},

		{"EndChunk a.mpg-1 (sess1)", EndChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 8, 0, time.UTC),
			SessionID: "sess1",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
			Index:     1,
			ChunkSize: 10,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 8, 0, time.UTC)
			stat.Origin.Bps = 0
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 50, CurSessionCount: 2}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheMissCount: 2, OriginBps: 0, CurSize: 30}
		}},

		{"StartChunk a.mpg-0, hit (sess1)", StartChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 8, 1, time.UTC),
			SessionID: "sess1",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
			Index:     0,
			ChunkSize: 20,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 8, 1, time.UTC)
			stat.Origin.Bps = 0
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 50, CurSessionCount: 2}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 2, OriginBps: 0, CurSize: 30}
		}},

		{"EndChunk a.mpg-0, hit (sess1)", EndChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 8, 2, time.UTC),
			SessionID: "sess1",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
			Index:     0,
			ChunkSize: 20,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 8, 2, time.UTC)
			stat.Origin.Bps = 0
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 50, CurSessionCount: 2}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 2, OriginBps: 0, CurSize: 30}
		}},

		{"EndSession sess1", EndSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 9, 0, time.UTC),
			SessionID: "sess1",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
		}, false, stat, func() {
			stat.Origin.Bps = 0
			stat.Time = time.Date(2017, 1, 1, 0, 0, 9, 0, time.UTC)
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 20, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 2, OriginBps: 0, CurSize: 30}
		}},

		{"EndSession sess2", EndSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 12, 0, time.UTC),
			SessionID: "sess2",
			FileName:  "b.mpg",
			FileSize:  20,
			Bps:       20,
		}, false, stat, func() {
			stat.Origin.Bps = 0
			stat.Time = time.Date(2017, 1, 1, 0, 0, 12, 0, time.UTC)
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 0, CurSessionCount: 0}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 2, OriginBps: 0, CurSize: 30}
		}},

		{"StartSession sess3", StartSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 15, 0, time.UTC),
			SessionID: "sess3",
			FileName:  "d.mpg",
			FileSize:  80,
			Bps:       40,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 15, 0, time.UTC)
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 40, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 2, OriginBps: 0, CurSize: 30}
		}},

		{"StartChunk d.mpg-0, miss (sess3), cache full and evicted", StartChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 15, 1, time.UTC),
			SessionID: "sess3",
			FileName:  "d.mpg",
			FileSize:  80,
			Bps:       40,
			Index:     0,
			ChunkSize: 80,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 15, 1, time.UTC)
			stat.Origin.Bps = 40
			stat.AllCacheFull = true
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 40, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 3, OriginBps: 40, CurSize: 100}
		}},

		{"EndChunk d.mpg-0, miss (sess3)", EndChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 15, 2, time.UTC),
			SessionID: "sess3",
			FileName:  "d.mpg",
			FileSize:  80,
			Bps:       40,
			Index:     0,
			ChunkSize: 80,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 15, 2, time.UTC)
			stat.Origin.Bps = 0
			stat.AllCacheFull = true
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 40, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 3, OriginBps: 0, CurSize: 100}
		}},

		{"EndSession sess3", EndSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 15, 3, time.UTC),
			SessionID: "sess3",
			FileName:  "d.mpg",
			FileSize:  80,
			Bps:       40,
		}, false, stat, func() {
			stat.Origin.Bps = 0
			stat.AllCacheFull = true
			stat.Time = time.Date(2017, 1, 1, 0, 0, 15, 3, time.UTC)
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 0, CurSessionCount: 0}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 3, OriginBps: 0, CurSize: 100}
		}},

		{"StartSession sess4", StartSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 18, 0, time.UTC),
			SessionID: "sess4",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 18, 0, time.UTC)
			stat.AllCacheFull = true
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 30, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 3, OriginBps: 0, CurSize: 100}
		}},

		{"StartChunk a.mpg-1, miss (sess4), a.mpg-0 cached and evicted", StartChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 18, 1, time.UTC),
			SessionID: "sess4",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
			Index:     1,
			ChunkSize: 10,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 18, 1, time.UTC)
			stat.Origin.Bps = 30
			stat.AllCacheFull = true
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 30, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 4, OriginBps: 30, CurSize: 90}
		}},

		{"EndChunk a.mpg-1, miss (sess4)", EndChunkType, data.ChunkEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 21, 0, time.UTC),
			SessionID: "sess4",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
			Index:     1,
			ChunkSize: 10,
		}, false, stat, func() {
			stat.Time = time.Date(2017, 1, 1, 0, 0, 21, 0, time.UTC)
			stat.Origin.Bps = 0
			stat.AllCacheFull = true
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 30, CurSessionCount: 1}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 4, OriginBps: 0, CurSize: 90}
		}},

		{"EndSession sess4", EndSessionType, data.SessionEvent{
			Time:      time.Date(2017, 1, 1, 0, 0, 21, 1, time.UTC),
			SessionID: "sess4",
			FileName:  "a.mpg",
			FileSize:  30,
			Bps:       30,
		}, false, stat, func() {
			stat.Origin.Bps = 0
			stat.AllCacheFull = true
			stat.Time = time.Date(2017, 1, 1, 0, 0, 21, 1, time.UTC)
			stat.Vods[vod.Key(v1.VodID)] = &status.VODStatus{VODKey: v1.VodID, CurBps: 0, CurSessionCount: 0}
			stat.Caches[vod.Key(v1.VodID)] = &status.CacheStatus{VODKey: v1.VodID, CacheHitCount: 1, CacheMissCount: 4, OriginBps: 0, CurSize: 90}
		}},
	}

	for _, tt := range tests {
		if tt.setupFn != nil {
			tt.setupFn()
		}
		var st *status.Status
		var err error
		switch tt.eventType {
		case StartSessionType:
			st, err = lb.StartSession(tt.event.(data.SessionEvent))
		case EndSessionType:
			st, err = lb.EndSession(tt.event.(data.SessionEvent))
		case StartChunkType:
			st, err = lb.StartChunk(tt.event.(data.ChunkEvent))
		case EndChunkType:
			st, err = lb.EndChunk(tt.event.(data.ChunkEvent))
		}
		if tt.expectError {
			assert.NotNil(err, tt.name)
		} else {
			assert.Nil(err, tt.name)
		}
		assert.Equal(tt.expectStatus, st, tt.name)
	}
}
