package vod

import (
	"sync"

	"github.com/castisdev/cdn-simul/data"
)

// Key :
type Key string

// VOD :
type VOD struct {
	mu                sync.RWMutex
	curSessionCount   int64
	limitSessionCount int64
	curBps            int64
	limitBps          int64
}

// StartSession :
func (v *VOD) StartSession(evt data.SessionEvent) error {
	return nil
}

// EndSession :
func (v *VOD) EndSession(evt data.SessionEvent) error {
	return nil
}
