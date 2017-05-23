package cache

import "container/list"

// Lru :
type Lru struct {
	MaxEntries int
	OnEvicted  func(key lruKey, value interface{})
	OnCacheHit func(key lruKey)

	ll    *list.List
	cache map[interface{}]*list.Element
}

type lruKey interface{}
type lruEntry struct {
	key   lruKey
	value interface{}
}

// NewLru :
func NewLru(maxEntries int) *Lru {
	return &Lru{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
	}
}

// Add :
func (c *Lru) Add(key lruKey, value interface{}) {
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
	}
	if ee, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ee)
		ee.Value.(*lruEntry).value = value
		return
	}
	ele := c.ll.PushFront(&lruEntry{key, value})
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

// Get :
func (c *Lru) Get(key lruKey) (value interface{}, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.ll.MoveToFront(ele)
		if c.OnCacheHit != nil {
			c.OnCacheHit(key)
		}
		return ele.Value.(*lruEntry).value, true
	}
	return
}

// Remove :
func (c *Lru) Remove(key lruKey) bool {
	if c.cache == nil {
		return false
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
		return true
	}
	return false
}

// RemoveOldest :
func (c *Lru) RemoveOldest() {
	if c.cache == nil {
		return
	}
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
	}
}

func (c *Lru) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*lruEntry)
	delete(c.cache, kv.key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.key, kv.value)
	}
}

// Len :
func (c *Lru) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}
