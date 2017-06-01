package glblog

import (
	"fmt"
	"time"
)

var layout = "2006-01-02 15:04:05.000"
var dateLayout = "2006-01-02"

////////////////////////////////////////////////////////////////////////////////

// EventType :
type EventType int

func (et EventType) String() string {
	switch et {
	case SessionCreated:
		return "s created"
	case SessionClosed:
		return "s closed"
	case ChunkCreated:
		return "c created"
	case ChunkClosed:
		return "c closed"
	default:
		return "unknown"
	}
}

const (
	// ChunkClosed :
	ChunkClosed EventType = iota
	// SessionClosed :
	SessionClosed
	// SessionCreated :
	SessionCreated
	// ChunkCreated :
	ChunkCreated
)

// Event :
type Event struct {
	SID       string
	EventTime time.Time
	Filename  string
	Index     int
	EventType EventType
}

func (e Event) String() string {
	return fmt.Sprintf("%s, %s, %9v, %4d, %s", e.EventTime.Format(layout), e.SID, e.EventType, e.Index, e.Filename)
}

// EventSorter :
type EventSorter []Event

func (lis EventSorter) Len() int {
	return len(lis)
}
func (lis EventSorter) Swap(i, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}
func (lis EventSorter) Less(i, j int) bool {
	return lis[i].EventTime.Before(lis[j].EventTime)
}
