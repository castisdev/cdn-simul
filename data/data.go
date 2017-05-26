package data

import (
	"fmt"
	"strings"
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
	HitResetTimes []JSONTime  `json:"hitResetTimes"`
	VODs          []VODConfig `json:"vods"`
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
	Time      time.Time
	SessionID string
	FileName  string
	FileSize  int64
	Bps       int64
}

func (s *SessionEvent) String() string {
	layout := "2006-01-02 15:04:05.000"
	return fmt.Sprintf("Session %s %s %s size:%d bps:%d", s.Time.Format(layout), s.SessionID, s.FileName, s.FileSize, s.Bps)
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

func (s *ChunkEvent) String() string {
	layout := "2006-01-02 15:04:05.000"
	return fmt.Sprintf("Chunk %s %s %s size:%d bps:%d idx:%d chunksize:%d",
		s.Time.Format(layout), s.SessionID, s.FileName, s.FileSize, s.Bps, s.Index, s.ChunkSize)
}

// JSONTime :
type JSONTime time.Time

// MarshalJSON :
func (t JSONTime) MarshalJSON() ([]byte, error) {
	var layout = "2006-01-02 15:04:05.000"
	stamp := time.Time(t).Format(layout)
	return []byte(stamp), nil
}

// UnmarshalJSON :
func (t *JSONTime) UnmarshalJSON(b []byte) error {
	var layout = "2006-01-02 15:04:05.000"
	loc, _ := time.LoadLocation("Local")
	tt, err := time.ParseInLocation(layout, strings.Trim(string(b), "\""), loc)
	if err != nil {
		return fmt.Errorf("failed to parse time, %v", err)
	}
	*t = JSONTime(tt)
	return nil
}
