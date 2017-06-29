package lb

import (
	"fmt"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb/cache"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn-simul/status"
)

// LB :
type LB struct {
	Caches        map[vod.Key]*cache.Cache
	VODs          map[vod.Key]*vod.VOD
	vodSessionMap map[string]vod.Key
	Selector      VODSelector
}

// LoadBalancer :
type LoadBalancer interface {
	StartSession(evt *data.SessionEvent) error
	EndSession(evt *data.SessionEvent) error
	StartChunk(evt *data.ChunkEvent) (useOrigin bool, err error)
	EndChunk(evt *data.ChunkEvent, useOrigin bool) error
	GetVODs() map[vod.Key]*vod.VOD
	Status(t time.Time) *status.Status
}

// New :
func New(cfg data.Config, selector VODSelector) (LoadBalancer, error) {
	l := &LB{
		Caches:        make(map[vod.Key]*cache.Cache),
		VODs:          make(map[vod.Key]*vod.VOD),
		vodSessionMap: make(map[string]vod.Key),
		Selector:      selector,
	}
	for _, v := range cfg.VODs {
		c, err := cache.NewCache(v.StorageSize)
		if err != nil {
			return nil, err
		}
		l.Caches[vod.Key(v.VodID)] = c
		l.VODs[vod.Key(v.VodID)] = &vod.VOD{LimitSessionCount: v.LimitSession, LimitBps: v.LimitBps}
	}

	l.Selector.Init(cfg)
	return l, nil
}

// GetVODs :
func (lb *LB) GetVODs() map[vod.Key]*vod.VOD {
	return lb.VODs
}

// SelectVOD :
func (lb *LB) SelectVOD(evt *data.SessionEvent) (vod.Key, error) {
	if len(lb.VODs) != len(lb.Caches) || len(lb.VODs) == 0 || len(lb.Caches) == 0 {
		return "", fmt.Errorf("invalid cache/vod info")
	}
	return lb.Selector.VODSelect(evt, lb)
}

// Status :
func (lb *LB) Status(t time.Time) *status.Status {
	return lb.MakeStatus(t)
}

// StartSession :
func (lb *LB) StartSession(evt *data.SessionEvent) error {
	key, err := lb.SelectVOD(evt)
	if err != nil {
		return fmt.Errorf("failed to select VOD, %v", err)
	}
	err = lb.VODs[key].StartSession(evt)
	if err != nil {
		return fmt.Errorf("failed to start session in VOD, %v", err)
	}
	lb.vodSessionMap[evt.SessionID] = key
	return nil
}

// EndSession :
func (lb *LB) EndSession(evt *data.SessionEvent) error {
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
func (lb *LB) StartChunk(evt *data.ChunkEvent) (useOrigin bool, err error) {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return false, fmt.Errorf("not exists session %v", evt.SessionID)
	}
	useOrigin, err = lb.Caches[key].StartChunk(evt)
	if err != nil {
		return false, fmt.Errorf("failed to start chunk in cache, %v", err)
	}
	return useOrigin, err
}

// EndChunk :
func (lb *LB) EndChunk(evt *data.ChunkEvent, useOrigin bool) error {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return fmt.Errorf("not exists session %v", evt.SessionID)
	}
	err := lb.Caches[key].EndChunk(evt, useOrigin)
	if err != nil {
		return fmt.Errorf("failed to end chunk in cache, %v", err)
	}
	return nil
}

// MakeStatus :
func (lb *LB) MakeStatus(t time.Time) *status.Status {
	st := &status.Status{
		Time:   t,
		Origin: &status.OriginStatus{},
		Vods:   make(map[vod.Key]*status.VODStatus),
		Caches: make(map[vod.Key]*status.CacheStatus),
	}
	originBps := int64(0)
	allCacheFull := true
	for k, v := range lb.Caches {
		originBps += v.OriginBps

		st.Caches[k] = &status.CacheStatus{
			VODKey:         string(k),
			CacheHitCount:  v.HitCount,
			CacheMissCount: v.MissCount,
			OriginBps:      v.OriginBps,
			CurSize:        v.CurSize,
		}
		if !v.IsCacheFull {
			allCacheFull = false
		}
	}
	st.AllCacheFull = allCacheFull
	st.Origin.Bps = originBps
	for k, v := range lb.VODs {
		st.Vods[k] = &status.VODStatus{
			VODKey:          string(k),
			CurBps:          v.CurBps,
			CurSessionCount: v.CurSessionCount,
		}
	}
	return st
}
