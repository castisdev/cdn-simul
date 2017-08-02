package lb

import (
	"fmt"
	"time"

	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/lb/vod"
	"github.com/castisdev/cdn-simul/status"
)

// FilebaseLB :
type FilebaseLB struct {
	VODs          map[vod.Key]*vod.VOD
	vodSessionMap map[string]vod.Key
	selector      VODSelector
	HitCount      int64
	MissCount     int64
	OriginBps     int64
}

// NewFilebaseLB :
func NewFilebaseLB(cfg data.Config, selector VODSelector) (LoadBalancer, error) {
	fmt.Println("FilebaseLB created")
	l := &FilebaseLB{
		VODs:          make(map[vod.Key]*vod.VOD),
		vodSessionMap: make(map[string]vod.Key),
		selector:      selector,
	}

	// 1개의 VOD만 있다고 가정
	for _, v := range cfg.VODs {
		l.VODs[vod.Key(v.VodID)] = &vod.VOD{LimitSessionCount: v.LimitSession, LimitBps: v.LimitBps}
		break
	}
	if len(l.VODs) != 1 {
		return nil, fmt.Errorf("invalid vod info")
	}
	err := l.selector.Init(cfg)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// GetVODs :
func (lb *FilebaseLB) GetVODs() map[vod.Key]*vod.VOD {
	return lb.VODs
}

// Status :
func (lb *FilebaseLB) Status(t time.Time) *status.Status {
	return lb.MakeStatus(t)
}

// StartSession :
func (lb *FilebaseLB) StartSession(evt *data.SessionEvent) error {
	k, err := lb.selector.VODSelect(evt, lb)
	if err == ErrFileNotFound {
		// file 없으면 StartChunk 시 cache miss 처리
		for _, v := range lb.VODs {
			// 하나의 VOD만 있다고 가정
			v.HitFail()
			break
		}
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to select VOD, %v", err)
	}

	err = lb.VODs[k].StartSession(evt)
	if err != nil {
		return fmt.Errorf("failed to start session in VOD, %v", err)
	}
	lb.vodSessionMap[evt.SessionID] = k
	return nil
}

// EndSession :
func (lb *FilebaseLB) EndSession(evt *data.SessionEvent) error {
	lb.selector.EndSession(evt)
	key, ok := lb.vodSessionMap[evt.SessionID]
	if ok {
		err := lb.VODs[key].EndSession(evt)
		if err != nil {
			return fmt.Errorf("failed to end session in VOD, %v", err)
		}
		delete(lb.vodSessionMap, evt.SessionID)
	}
	return nil
}

// StartChunk :
func (lb *FilebaseLB) StartChunk(evt *data.ChunkEvent) (useOrigin bool, err error) {
	_, ok := lb.vodSessionMap[evt.SessionID]
	if ok {
		lb.HitCount++
	} else {
		lb.MissCount++
		lb.OriginBps += evt.Bps
	}
	return !ok, nil
}

// EndChunk :
func (lb *FilebaseLB) EndChunk(evt *data.ChunkEvent, useOrigin bool) error {
	if useOrigin {
		lb.OriginBps -= evt.Bps
	}
	return nil
}

// MakeStatus :
func (lb *FilebaseLB) MakeStatus(t time.Time) *status.Status {
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
			VODKey:            string(k),
			CurBps:            v.CurBps,
			CurSessionCount:   v.CurSessionCount,
			TotalSessionCount: v.TotalSessionCount,
			HitSessionCount:   v.HitSessionCount,
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
