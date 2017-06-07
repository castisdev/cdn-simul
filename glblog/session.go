package glblog

import (
	"fmt"
	"time"
)

// SessionInfo :
type SessionInfo struct {
	SID       string
	Started   time.Time
	Ended     time.Time
	Filename  string
	Bandwidth int
	Offset    int64
}

func (s SessionInfo) String() string {
	return fmt.Sprintf("%s, %s, %s, %s, %d, %d", s.Started.Format(layout), s.SID, s.Ended.Format(layout), s.Filename, s.Bandwidth, s.Offset)
}
