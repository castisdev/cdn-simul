package data

import (
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
	VODs []VODConfig
}

// VODConfig :
type VODConfig struct {
	VodID        string
	StorageSize  int64
	LimitSession int64
	LimitBps     int64
}

// SessionEvent :
type SessionEvent struct {
	Time      time.Time
	SessionID string
	FileName  string
	FileSize  int64
	Bps       int64
}

// ChunkEvent :
type ChunkEvent struct {
	Time      time.Time
	SessionID string
	FileName  string
	FileSize  int64
	Bps       int64
	Index     int64
	ChunkSize int64
}
