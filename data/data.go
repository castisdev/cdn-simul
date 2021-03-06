package data

import (
	"fmt"
	"time"
)

// Session :
type Session struct {
	ID        string
	StartTime time.Time
	EndTime   time.Time
	FileName  string
	FileSize  int64
	Bps       int64
}

// Config :
type Config struct {
	VODs []VODConfig `json:"vods"`
}

// VODConfig :
type VODConfig struct {
	VodID        string `json:"vodid"`
	StorageSize  int64  `json:"storageSize"`
	LimitSession int64  `json:"limitSession"`
	LimitBps     int64  `json:"limitBps"`
}

// SessionEvent :
type SessionEvent struct {
	Time        time.Time
	SessionID   string
	FileName    string
	IntFileName int
	FileSize    int64
	Bps         int64
	Duration    time.Duration
}

func (s SessionEvent) String() string {
	layout := "2006-01-02 15:04:05.000"
	return fmt.Sprintf("Session %s %s %s(%d) size:%d bps:%d duration:%v",
		s.Time.Format(layout), s.SessionID, s.FileName, s.IntFileName, s.FileSize, s.Bps, s.Duration)
}

// ChunkEvent :
type ChunkEvent struct {
	Time        time.Time
	SessionID   string
	FileName    string
	IntFileName int
	FileSize    int64
	Bps         int64
	Index       int64
	ChunkSize   int64
	Bypass      bool
	IsCenter    bool
}

func (s ChunkEvent) String() string {
	layout := "2006-01-02 15:04:05.000"
	return fmt.Sprintf("Chunk %s %s %s size:%d bps:%d idx:%d chunksize:%d bypass:%v center:%v",
		s.Time.Format(layout), s.SessionID, s.FileName, s.FileSize, s.Bps, s.Index, s.ChunkSize, s.Bypass, s.IsCenter)
}

// DeliverEvent :
type DeliverEvent struct {
	Time     time.Time
	FileName string
	FileSize int64
}

// PurgeEvent :
type PurgeEvent struct {
	Time     time.Time
	FileName string
}
