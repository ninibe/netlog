// generated file - DO NOT EDIT
// command: atomicmapper -pointer -type TopicScanner

package netlog

import (
	"sync"
	"sync/atomic"
)

// TopicScannerAtomicMap is a copy-on-write thread-safe map of pointers to TopicScanner
type TopicScannerAtomicMap struct {
	mu  sync.Mutex
	val atomic.Value
}

type _TopicScannerMap map[string]*TopicScanner

// NewTopicScannerAtomicMap returns a new initialized TopicScannerAtomicMap
func NewTopicScannerAtomicMap() *TopicScannerAtomicMap {
	am := &TopicScannerAtomicMap{}
	am.val.Store(make(_TopicScannerMap, 0))
	return am
}

// Get returns a pointer to TopicScanner for a given key
func (am *TopicScannerAtomicMap) Get(key string) (value *TopicScanner, ok bool) {
	value, ok = am.val.Load().(_TopicScannerMap)[key]
	return value, ok
}

// GetAll returns the underlying map of pointers to TopicScanner
// this map must NOT be modified, to change the map safely use the Set and Delete
// functions and Get the value again
func (am *TopicScannerAtomicMap) GetAll() map[string]*TopicScanner {
	return am.val.Load().(_TopicScannerMap)
}

// Len returns the number of elements in the map
func (am *TopicScannerAtomicMap) Len() int {
	return len(am.val.Load().(_TopicScannerMap))
}

// Set inserts in the map a pointer to TopicScanner under a given key
func (am *TopicScannerAtomicMap) Set(key string, value *TopicScanner) {
	am.mu.Lock()
	defer am.mu.Unlock()

	m1 := am.val.Load().(_TopicScannerMap)
	m2 := make(_TopicScannerMap, len(m1)+1)
	for k, v := range m1 {
		m2[k] = v
	}

	m2[key] = value
	am.val.Store(m2)
	return
}

// Delete removes the pointer to TopicScanner under key from the map
func (am *TopicScannerAtomicMap) Delete(key string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	m1 := am.val.Load().(_TopicScannerMap)
	_, ok := m1[key]
	if !ok {
		return
	}

	m2 := make(_TopicScannerMap, len(m1)-1)
	for k, v := range m1 {
		if k != key {
			m2[k] = v
		}
	}

	am.val.Store(m2)
	return
}
