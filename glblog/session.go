package glblog

import (
	"fmt"
	"time"
)

// SessionInfo :
type SessionInfo struct {
	SID         string
	Started     time.Time
	Ended       time.Time
	Filename    string
	Bandwidth   int
	Offset      int64
	Filesize    int64
	IsCenter    bool
	IsSemiSetup bool
}

func (s SessionInfo) String() string {
	return fmt.Sprintf("%s, %s, %s, %s, %d, %d, %d, %v, %v", s.Started.Format(layout), s.SID, s.Ended.Format(layout), s.Filename, s.Bandwidth, s.Offset, s.Filesize, s.IsCenter, s.IsSemiSetup)
}
