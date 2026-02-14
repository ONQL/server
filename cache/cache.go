package cache

import (
	"container/list"
	"sync"
	"time"
)

// Cache is a basic LRU cache with TTL enforcement and a byte budget.
type Cache struct {
	maxBytes int64
	ttl      time.Duration

	mu           sync.Mutex
	entries      map[string]*list.Element
	ll           *list.List
	currentBytes int64
}

type entry struct {
	key       string
	value     string
	size      int64
	timestamp time.Time
}

// New creates a cache capped at maxBytes. When maxBytes <= 0, caching is disabled.
func New(maxBytes int64, ttl time.Duration) *Cache {
	if maxBytes <= 0 {
		return &Cache{maxBytes: 0}
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &Cache{
		maxBytes: maxBytes,
		ttl:      ttl,
		entries:  make(map[string]*list.Element),
		ll:       list.New(),
	}
}

// Enabled reports whether the cache is active.
func (c *Cache) Enabled() bool {
	return c != nil && c.maxBytes > 0
}

// Get fetches a cached value when present and fresh.
func (c *Cache) Get(key string) (string, bool) {
	if !c.Enabled() {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return "", false
	}
	ent := elem.Value.(*entry)
	if time.Since(ent.timestamp) > c.ttl {
		c.removeElement(elem)
		return "", false
	}
	c.ll.MoveToFront(elem)
	return ent.value, true
}

// Set stores a value if it fits inside the configured byte budget.
func (c *Cache) Set(key, value string) {
	if !c.Enabled() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	size := int64(len(key) + len(value))
	if size > c.maxBytes {
		return
	}

	if elem, ok := c.entries[key]; ok {
		ent := elem.Value.(*entry)
		c.currentBytes -= ent.size
		ent.value = value
		ent.size = size
		ent.timestamp = time.Now()
		c.currentBytes += size
		c.ll.MoveToFront(elem)
		c.evict()
		return
	}

	ent := &entry{key: key, value: value, size: size, timestamp: time.Now()}
	elem := c.ll.PushFront(ent)
	c.entries[key] = elem
	c.currentBytes += size
	c.evict()
}

// Clear removes every cached entry.
func (c *Cache) Clear() {
	if !c.Enabled() {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	c.entries = make(map[string]*list.Element)
	c.currentBytes = 0
}

func (c *Cache) evict() {
	for c.currentBytes > c.maxBytes && c.ll.Len() > 0 {
		c.removeElement(c.ll.Back())
	}
}

func (c *Cache) removeElement(elem *list.Element) {
	if elem == nil {
		return
	}
	ent := elem.Value.(*entry)
	delete(c.entries, ent.key)
	c.ll.Remove(elem)
	c.currentBytes -= ent.size
	if c.currentBytes < 0 {
		c.currentBytes = 0
	}
}
