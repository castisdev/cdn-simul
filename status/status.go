package status

import (
	"time"

	"github.com/castisdev/cdn-simul/lb/vod"
)

// Status :
type Status struct {
	Time         time.Time
	Origin       *OriginStatus
	Vods         map[vod.Key]*VODStatus
	Caches       map[vod.Key]*CacheStatus
	AllCacheFull bool
}

// OriginStatus :
type OriginStatus struct {
	Bps int64
}

// VODStatus :
type VODStatus struct {
	VODKey            string
	CurSessionCount   int64
	CurBps            int64
	TotalSessionCount int64
	HitSessionCount   int64
}

// CacheStatus :
type CacheStatus struct {
	VODKey         string
	CacheMissCount int64
	CacheHitCount  int64
	OriginBps      int64
	CurSize        int64
}
