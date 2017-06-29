package lb

import (
	"fmt"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn-simul/status"
)

// LegacyLB :
type LegacyLB struct {
	VODs          map[vod.Key]*vod.VOD
	vodSessionMap map[string]vod.Key
	HitCount      int64
	MissCount     int64
	OriginBps     int64
}

// NewLegacyLB :
func NewLegacyLB(cfg data.Config, selector VODSelector) (LoadBalancer, error) {
	l := &LegacyLB{
		VODs:          make(map[vod.Key]*vod.VOD),
		vodSessionMap: make(map[string]vod.Key),
	}

	// 1개의 VOD만 있다고 가정
	for _, v := range cfg.VODs {
		l.VODs[vod.Key(v.VodID)] = &vod.VOD{LimitSessionCount: v.LimitSession, LimitBps: v.LimitBps}
		break
	}
	if len(l.VODs) != 1 {
		return nil, fmt.Errorf("invalid vod info")
	}

	return l, nil
}

// GetVODs :
func (lb *LegacyLB) GetVODs() map[vod.Key]*vod.VOD {
	return lb.VODs
}

// Status :
func (lb *LegacyLB) Status(t time.Time) *status.Status {
	return lb.MakeStatus(t)
}

// StartSession :
func (lb *LegacyLB) StartSession(evt *data.SessionEvent) error {
	// 1개의 VOD만 있다고 가정
	var firstK vod.Key
	for k := range lb.VODs {
		firstK = k
		break
	}

	err := lb.VODs[firstK].StartSession(evt)
	if err != nil {
		return fmt.Errorf("failed to start session in VOD, %v", err)
	}
	lb.vodSessionMap[evt.SessionID] = firstK
	return nil
}

// EndSession :
func (lb *LegacyLB) EndSession(evt *data.SessionEvent) error {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return fmt.Errorf("not exists session %v", evt.SessionID)
	}
	err := lb.VODs[key].EndSession(evt)
	if err != nil {
		return fmt.Errorf("failed to end session in VOD, %v", err)
	}
	delete(lb.vodSessionMap, evt.SessionID)
	return nil
}

// StartChunk :
func (lb *LegacyLB) StartChunk(evt *data.ChunkEvent) (useOrigin bool, err error) {
	_, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return false, fmt.Errorf("not exists session %v", evt.SessionID)
	}

	if evt.IsCenter {
		lb.MissCount++
		lb.OriginBps += evt.Bps
	} else {
		lb.HitCount++
	}
	return evt.IsCenter, nil
}

// EndChunk :
func (lb *LegacyLB) EndChunk(evt *data.ChunkEvent, useOrigin bool) error {
	_, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return fmt.Errorf("not exists session %v", evt.SessionID)
	}
	if evt.IsCenter {
		lb.OriginBps -= evt.Bps
	}
	return nil
}

// MakeStatus :
func (lb *LegacyLB) MakeStatus(t time.Time) *status.Status {
	st := &status.Status{
		Time:   t,
		Origin: &status.OriginStatus{},
		Vods:   make(map[vod.Key]*status.VODStatus),
		Caches: make(map[vod.Key]*status.CacheStatus),
	}
	st.AllCacheFull = true
	st.Origin.Bps = lb.OriginBps
	for k, v := range lb.VODs {
		st.Vods[k] = &status.VODStatus{
			VODKey:          string(k),
			CurBps:          v.CurBps,
			CurSessionCount: v.CurSessionCount,
		}
		st.Caches[k] = &status.CacheStatus{
			VODKey:         string(k),
			CacheHitCount:  lb.HitCount,
			CacheMissCount: lb.MissCount,
			OriginBps:      lb.OriginBps,
			CurSize:        0,
		}
	}
	return st
}
