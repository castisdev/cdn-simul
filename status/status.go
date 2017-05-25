package status

import (
	"fmt"
	"time"

	"github.com/castisdev/cdn-simul/lb/vod"
)

// Status :
type Status struct {
	Time   time.Time
	Origin *OriginStatus
	Vods   map[vod.Key]*VODStatus
	Caches map[vod.Key]*CacheStatus
}

func (s *Status) String() string {
	layout := "2006-01-02 15:04:05.000"
	ret := ""
	for _, v := range s.Vods {
		ret += fmt.Sprintf("vod[%s] ", v.String())
	}
	for _, v := range s.Caches {
		ret += fmt.Sprintf("cache[%s] ", v.String())
	}
	return fmt.Sprintf("%s origin[%s] %s", s.Time.Format(layout), s.Origin, ret)
}

// OriginStatus :
type OriginStatus struct {
	Bps int64
}

func (s *OriginStatus) String() string {
	return fmt.Sprintf("bps:%v", s.Bps)
}

// VODStatus :
type VODStatus struct {
	SessionCount int64
	Bps          int64
}

func (s *VODStatus) String() string {
	return fmt.Sprintf("session:%v bps:%v", s.SessionCount, s.Bps)
}

// CacheStatus :
type CacheStatus struct {
	CacheMissCount int64
	CacheHitCount  int64
	CacheMissRate  float64
	OriginBps      int64
}

func (s *CacheStatus) String() string {
	return fmt.Sprintf("miss:%v hit:%v missRate:%v originBps:%v", s.CacheMissCount, s.CacheHitCount, s.CacheMissRate, s.OriginBps)
}
