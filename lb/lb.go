package lb

import (
	"fmt"

	"github.com/castisdev/cdn-simul/cache"
	"github.com/castisdev/cdn-simul/data"
	"github.com/castisdev/cdn-simul/status"
	"github.com/castisdev/cdn-simul/vod"
)

// LB :
type LB struct {
	Caches        map[vod.Key]*cache.Cache
	VODs          map[vod.Key]*vod.VOD
	vodSessionMap map[string]vod.Key
}

// New :
func New(cfg data.Config) (*LB, error) {
	return nil, nil
}

// SelectVOD :
func (lb *LB) SelectVOD(evt data.SessionEvent) (vodKey vod.Key, err error) {
	return "", nil
}

// StartSession :
func (lb *LB) StartSession(evt data.SessionEvent) (*status.Status, error) {
	key, _ := lb.SelectVOD(evt)
	lb.vodSessionMap[evt.SessionID] = key
	lb.VODs[key].StartSession(evt)
	return nil, nil
}

// EndSession :
func (lb *LB) EndSession(evt data.SessionEvent) (*status.Status, error) {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return nil, fmt.Errorf("not exists session %v", evt.SessionID)
	}
	lb.VODs[key].EndSession(evt)
	delete(lb.vodSessionMap, evt.SessionID)
	return nil, nil
}

// StartChunk :
func (lb *LB) StartChunk(evt data.ChunkEvent) (*status.Status, error) {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return nil, fmt.Errorf("not exists session %v", evt.SessionID)
	}
	lb.Caches[key].StartChunk(evt)
	return nil, nil
}

// EndChunk :
func (lb *LB) EndChunk(evt data.ChunkEvent) (*status.Status, error) {
	key, ok := lb.vodSessionMap[evt.SessionID]
	if !ok {
		return nil, fmt.Errorf("not exists session %v", evt.SessionID)
	}
	lb.Caches[key].EndChunk(evt)
	return nil, nil
}
