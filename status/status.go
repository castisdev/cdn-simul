package status

import (
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

// OriginStatus :
type OriginStatus struct {
	Bps int64
}

// VODStatus :
type VODStatus struct {
	SessionCount int64
	Bps          int64
}

// CacheStatus :
type CacheStatus struct {
	CacheMissCount int64
	CacheHitCount  int64
	CacheMissRate  float64
	OriginBps      int64
}
