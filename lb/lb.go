package lb

import (
	"fmt"
	"log"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb/cache"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn-simul/status"
	"github.com/castisdev/cdn/consistenthash"
)

// LB :
type LB struct {
	Caches        map[vod.Key]*cache.Cache
	VODs          map[vod.Key]*vod.VOD
	vodSessionMap map[string]vod.Key

	hash *consistenthash.Map
}

// New :
func New(cfg data.Config) (*LB, error) {
	l := &LB{
		Caches:        make(map[vod.Key]*cache.Cache),
		VODs:          make(map[vod.Key]*vod.VOD),
		vodSessionMap: make(map[string]vod.Key),
	}
	l.hash = consistenthash.New(3000, nil)
	keyMap := make(map[string]int)
	for _, v := range cfg.VODs {
		c, err := cache.NewCache(v.StorageSize)
		if err != nil {
			return nil, err
		}
		l.Caches[vod.Key(v.VodID)] = c
		l.VODs[vod.Key(v.VodID)] = &vod.VOD{LimitSessionCount: v.LimitSession, LimitBps: v.LimitBps}
		hashWeight := 1
		keyMap[v.VodID] = hashWeight
	}
	l.hash.Add(keyMap)

	return l, nil
}

// SelectVOD :
func (lb *LB) SelectVOD(evt data.SessionEvent) (vod.Key, error) {
	if len(lb.VODs) != len(lb.Caches) || len(lb.VODs) == 0 || len(lb.Caches) == 0 {
		return "", fmt.Errorf("invalid cache/vod info")
	}
	return lb.SelectVODByHash(evt)
}

// SelectVODByHash :
func (lb *LB) SelectVODByHash(evt data.SessionEvent) (vod.Key, error) {
	vodKeys := lb.hash.GetItems(evt.FileName)
	for _, v := range vodKeys {
		k := vod.Key(v)
		if lb.VODs[k].LimitSessionCount < lb.VODs[k].CurSessionCount+1 || lb.VODs[k].LimitBps < lb.VODs[k].CurBps+evt.Bps {
			log.Printf("not available vod[%v], session(%v/%v) bps(%v/%v)",
				k, lb.VODs[k].CurSessionCount, lb.VODs[k].LimitSessionCount, lb.VODs[k].CurBps, lb.VODs[k].LimitBps)
			continue
		}
		return k, nil
	}
	return "", fmt.Errorf("failed to select vod")
}

// StartSession :
func (lb *LB) StartSession(evt data.SessionEvent) (*status.Status, error) {
	key, err := lb.SelectVOD(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to select VOD, %v", err)
	}
	err = lb.VODs[key].StartSession(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to start session in VOD, %v", err)
	}
	lb.vodSessionMap[evt.SessionID] = key
	return lb.MakeStatus(evt.Time), nil
}

// EndSession :
func (lb *LB) EndSession(evt data.SessionEvent) (*status.Status, error) {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return nil, fmt.Errorf("not exists session %v", evt.SessionID)
	}
	err := lb.VODs[key].EndSession(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to end session in VOD, %v", err)
	}
	delete(lb.vodSessionMap, evt.SessionID)
	return lb.MakeStatus(evt.Time), nil
}

// StartChunk :
func (lb *LB) StartChunk(evt data.ChunkEvent) (*status.Status, error) {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return nil, fmt.Errorf("not exists session %v", evt.SessionID)
	}
	err := lb.Caches[key].StartChunk(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to start chunk in cache, %v", err)
	}
	return lb.MakeStatus(evt.Time), nil
}

// EndChunk :
func (lb *LB) EndChunk(evt data.ChunkEvent) (*status.Status, error) {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return nil, fmt.Errorf("not exists session %v", evt.SessionID)
	}
	err := lb.Caches[key].EndChunk(evt)
	if err != nil {
		return nil, fmt.Errorf("failed to end chunk in cache, %v", err)
	}
	return lb.MakeStatus(evt.Time), nil
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
	for k, v := range lb.Caches {
		originBps += v.OriginBps

		st.Caches[k] = &status.CacheStatus{
			CacheHitCount:  v.HitCount,
			CacheMissCount: v.MissCount,
			OriginBps:      v.OriginBps,
			CurSize:        v.CurSize,
			LimitSize:      v.LimitSize,
		}
	}
	st.Origin.Bps = originBps
	for k, v := range lb.VODs {
		st.Vods[k] = &status.VODStatus{
			Bps:          v.CurBps,
			SessionCount: v.CurSessionCount,
		}
		miss := lb.Caches[k].MissCount
		hit := lb.Caches[k].HitCount
		if miss+hit == 0 {
			st.Caches[k].CacheMissRate = 0
		} else {
			st.Caches[k].CacheMissRate = float64(float64(miss) / float64(miss+hit) * 100)
		}
	}
	return st
}
