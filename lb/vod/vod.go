package vod

import (
	"fmt"

	"github.com/castisdev/cdn-simul/data"
)

// Key :
type Key string

// VOD :
type VOD struct {
	CurSessionCount   int64
	LimitSessionCount int64
	TotalSessionCount int64
	HitSessionCount   int64
	CurBps            int64
	LimitBps          int64
	TotalBps          int64
}

// HitFail :
func (v *VOD) HitFail() {
	// hit fail되더라도 session count 증가
	v.TotalSessionCount++
}

// StartSession :
func (v *VOD) StartSession(evt *data.SessionEvent) error {
	if v.CurSessionCount+1 > v.LimitSessionCount {
		return fmt.Errorf("reaches limit session count, cur(%v) limit(%v)", v.CurSessionCount, v.LimitSessionCount)
	}
	if v.CurBps+evt.Bps > v.LimitBps {
		return fmt.Errorf("reaches limit bps, cur(%v) limit(%v)", v.CurBps, v.LimitBps)
	}
	v.CurSessionCount++
	v.CurBps += evt.Bps
	v.TotalSessionCount++
	v.HitSessionCount++
	return nil
}

// EndSession :
func (v *VOD) EndSession(evt *data.SessionEvent) error {
	v.CurSessionCount--
	v.CurBps -= evt.Bps
	return nil
}
