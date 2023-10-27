package terraform

import (
	"log"
	"sync"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/zclconf/go-cty/cty"
)

type cached struct {
	input  *cty.Value
	output *cty.Value
}

type Cache struct {
	// config *configs.Module
	values map[string]*cached
	locks  LockMap
}

func NewCache() *Cache {
	return &Cache{
		// config: config,
		values: map[string]*cached{},
		locks:  NewLockMap(),
	}
}

func (c *Cache) getCached(addr addrs.ConfigResource) *cached {
	key := addr.String()
	c.locks.Lock(key)
	defer c.locks.Unlock(key)
	if v, ok := c.values[key]; ok {
		return v
	}

	v := &cached{}
	c.values[key] = v

	return v
}

func (c *Cache) GetCachedValue(resource addrs.ConfigResource, configVal cty.Value) (*cty.Value, error) {
	cached := c.getCached(resource)
	if cached.input == nil {
		log.Printf("[TRACE] Cache: data source cache init %s", resource)

		cached.input = &configVal

		return nil, nil
	}

	cmp := cached.input.Equals(configVal)
	if cmp.IsKnown() && cmp.True() {
		log.Printf("[TRACE] Cache: data source cache hit %s", resource)

		return cached.output, nil
	}

	log.Printf("[TRACE] Cache: data source cache miss %s", resource)

	cached.input = &configVal

	return nil, nil
}

func (c *Cache) SetCachedValue(resource addrs.ConfigResource, val *cty.Value) {
	log.Printf("[TRACE] Cache: caching result for %s", resource)
	cached := c.getCached(resource)
	cached.output = val
}

type LockMap struct {
	primary *sync.RWMutex
	locks   map[string]*sync.Mutex
}

func NewLockMap() LockMap {
	return LockMap{
		primary: &sync.RWMutex{},
		locks:   map[string]*sync.Mutex{},
	}
}

func (m *LockMap) Lock(key string) {
	m.primary.RLock()

	if l, ok := m.locks[key]; ok {
		m.primary.RUnlock()
		l.Lock()
	} else {
		m.primary.RUnlock()
		m.primary.Lock()
		defer m.primary.Unlock()

		if l, ok := m.locks[key]; ok {
			l.Lock()
		} else {
			l := &sync.Mutex{}
			m.locks[key] = l
			l.Lock()
		}
	}
}

func (m *LockMap) Unlock(key string) {
	m.primary.RLock()
	m.locks[key].Unlock()
	m.primary.RUnlock()
}
