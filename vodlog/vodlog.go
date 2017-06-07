package vodlog

import (
	"fmt"
	"time"
)

const (
	// MajorMask :
	MajorMask = 0xffff0000
	// MinorMask :
	MinorMask = 0x0000ffff
)

const (
	// SessionUsage :
	SessionUsage = 0x00010000
	// RtspListener :
	RtspListener = 0x00020000
	// RtspStreamer :
	RtspStreamer = 0x00040000
	// SessionManager :
	SessionManager = 0x00080000
	// FileManager :
	FileManager = 0x00100000
	// FSMP :
	FSMP = 0x00200000
	// Global :
	Global = 0x00400000
)

const (
	// Create :
	Create = 0x0001
	// Close :
	Close = 0x0002
	// FF :
	FF = 0x0004
	// RW :
	RW = 0x0008
	// Slow :
	Slow = 0x0010
	// Pause :
	Pause = 0x0020
	// Play :
	Play = 0x0040
	// Teardown :
	Teardown = 0x0080
	// Seek :
	Seek = 0x0100
	// Usage :
	Usage = 0x0200
)

// EventLog :
type EventLog struct {
	EventTime   time.Time
	SID         string
	Filename    string
	Bitrate     int
	Filesize    int64
	StartOffset int64
	Resetup     bool
	VodIP       string
	ClientIP    string
}

// Layout :
var Layout = "2006-01-02 15:04:05"

func (e EventLog) String() string {
	return fmt.Sprintf("%s, %s, %s, %38s, %11d, %11d, %8d, %t", e.EventTime.Format(Layout), e.SID, e.VodIP, e.Filename, e.Filesize, e.StartOffset, e.Bitrate, e.Resetup)
}

// Sorter :
type Sorter []EventLog

func (s Sorter) Len() int {
	return len(s)
}
func (s Sorter) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s Sorter) Less(i, j int) bool {
	if s[i].EventTime.Equal(s[j].EventTime) == false {
		return s[i].EventTime.Before(s[j].EventTime)
	}
	return s[i].StartOffset < s[j].StartOffset
}

func etypeString(etype int64) string {
	major := etype & 0xffff0000
	minor := etype & 0xffff
	var str string
	switch major {
	case SessionUsage:
		str = "SU/"
		switch minor {
		case Create:
			str += "create"
		case Close:
			str += "close"
		case FF:
			str += "ff"
		case RW:
			str += "rw"
		case Slow:
			str += "slow"
		case Pause:
			str += "pause"
		case Play:
			str += "play"
		case Teardown:
			str += "teardown"
		case Seek:
			str += "seek"
		case Usage:
			str += "usage"
		}
	case RtspListener:
		str = "RTSP-L"
	case RtspStreamer:
		str = "RTSP-S"
	case SessionManager:
		str = "SM"
	case FileManager:
		str = "FM"
	case FSMP:
		str = "FSMP"
	case Global:
		str = "GLOBAL"
	}
	return str
}
