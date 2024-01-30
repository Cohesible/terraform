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
	mu     sync.RWMutex
	locks  LockMap
	values map[string]*cached
}

func NewCache() *Cache {
	return &Cache{
		mu:     sync.RWMutex{},
		locks:  NewLockMap(),
		values: map[string]*cached{},
	}
}

func (c *Cache) getCached(key string) *cached {
	c.mu.RLock()
	if v, ok := c.values[key]; ok {
		c.mu.RUnlock()
		return v
	}

	c.mu.RUnlock()
	c.mu.Lock()

	// Need to check again as multiple readers could try to write
	if v, ok := c.values[key]; ok {
		c.mu.Unlock()
		return v
	}

	v := &cached{}
	c.values[key] = v
	c.mu.Unlock()

	return v
}

func (c *Cache) GetCachedValue(resource addrs.ConfigResource, configVal cty.Value) (*cty.Value, error) {
	key := resource.String()
	c.locks.Lock(key)
	defer c.locks.Unlock(key)

	cached := c.getCached(key)
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
	key := resource.String()
	c.locks.Lock(key)
	defer c.locks.Unlock(key)

	log.Printf("[TRACE] Cache: caching result for %s", resource)
	cached := c.getCached(key)
	cached.output = val
}

func (c *Cache) GetCachedValidation(provider addrs.AbsProviderConfig, configVal cty.Value) (*cty.Value, error) {
	key := provider.String()
	c.locks.Lock(key)
	defer c.locks.Unlock(key)

	cached := c.getCached(key)
	if cached.input == nil {
		log.Printf("[TRACE] Cache: validation cache init %s", provider)

		cached.input = &configVal

		return nil, nil
	}

	cmp := cached.input.Equals(configVal)
	if cmp.IsKnown() && cmp.True() {
		log.Printf("[TRACE] Cache: validation cache hit %s", provider)

		return cached.output, nil
	}

	log.Printf("[TRACE] Cache: validation cache miss %s", provider)

	cached.input = &configVal

	return nil, nil
}

func (c *Cache) SetCachedValidation(provider addrs.AbsProviderConfig, val *cty.Value) {
	key := provider.String()
	c.locks.Lock(key)
	defer c.locks.Unlock(key)

	log.Printf("[TRACE] Cache: caching validation for %s", provider)
	cached := c.getCached(key)
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
