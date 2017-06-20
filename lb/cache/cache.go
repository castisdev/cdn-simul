package cache

import (
	"fmt"

	"github.com/castisdev/cdn-simul/data"
)

// Cache :
type Cache struct {
	Lru         *Lru
	LimitSize   int64
	CurSize     int64
	HitCount    int64
	MissCount   int64
	OriginBps   int64
	IsCacheFull bool
}

// NewCache :
func NewCache(limitSize int64) (*Cache, error) {
	m := &Cache{
		LimitSize: limitSize,
	}

	if err := m.init(); err != nil {
		return nil, err
	}
	return m, nil
}

func filepath(evt *data.ChunkEvent) int {
	return evt.IntFileName*10000 + int(evt.Index)
}

// StartChunk :
func (c *Cache) StartChunk(evt *data.ChunkEvent) (useOrigin bool, err error) {
	if evt.Bypass {
		c.MissCount++
		c.OriginBps += evt.Bps
		useOrigin = false
		return useOrigin, err
	}
	key := filepath(evt)
	n, ok := c.Get(key)
	if ok {
		if n != evt.ChunkSize {
			return false, fmt.Errorf("invalid chunk size, cached(%v) evt(%v)", n, evt.ChunkSize)
		}
		c.HitCount++
	} else {
		err = c.Add(key, evt.ChunkSize)
		if err != nil {
			return false, err
		}
		c.MissCount++
		c.OriginBps += evt.Bps
		useOrigin = true
	}
	return useOrigin, err
}

// EndChunk :
func (c *Cache) EndChunk(evt *data.ChunkEvent, useOrigin bool) error {
	if useOrigin {
		c.OriginBps -= evt.Bps
	}
	return nil
}

func (c *Cache) init() error {
	c.Lru = &Lru{
		OnEvicted: func(key lruKey, value interface{}) {
			c.IsCacheFull = true
			v := value.(int64)
			c.CurSize -= v
		},
	}

	return nil
}

// Add :
func (c *Cache) Add(relFilePath int, size int64) error {
	key := relFilePath

	if c.LimitSize <= 0 || c.LimitSize < size {
		return fmt.Errorf("data size(%d) > cache limit size(%d)", size, c.LimitSize)
	}

	for {
		if c.CurSize+size <= c.LimitSize {
			break
		}
		c.Lru.RemoveOldest()
	}
	c.Lru.Add(key, size)
	c.CurSize += size
	return nil
}

// Get :
func (c *Cache) Get(filepath int) (size int64, ok bool) {
	if c.Lru == nil {
		return
	}
	v, ok := c.Lru.Get(filepath)
	if !ok {
		return
	}
	return v.(int64), true
}

// Remove :
func (c *Cache) Remove(filepath string) bool {
	return c.Lru.Remove(filepath)
}
