package cache

import (
	"fmt"
	"sync"

	"github.com/castisdev/cdn-simul/data"
)

// Cache :
type Cache struct {
	mu        sync.RWMutex
	Lru       *Lru
	LimitSize int64
	CurSize   int64
	HitCount  int64
	MissCount int64
	originBps int64
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

// StartChunk :
func (c *Cache) StartChunk(evt data.Event) error {
	return nil
}

// EndChunk :
func (c *Cache) EndChunk(evt data.Event) error {
	return nil
}

func (c *Cache) init() error {
	c.Lru = &Lru{
		OnEvicted: func(key lruKey, value interface{}) {
			v := value.(int64)
			c.CurSize -= v
		},
		OnCacheHit: nil,
	}

	return nil
}

// Add :
func (c *Cache) Add(relFilePath string, size int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

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
func (c *Cache) Get(filepath string) (size int64, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.Lru.Remove(filepath)
}
