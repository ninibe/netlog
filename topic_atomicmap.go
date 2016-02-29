// generated file - DO NOT EDIT
// command: atomicmapper -pointer -type Topic

package netlog

import (
	"sync"
	"sync/atomic"
)

// TopicAtomicMap is a copy-on-write thread-safe map of pointers to Topic
type TopicAtomicMap struct {
	mu  sync.Mutex
	val atomic.Value
}

type _TopicMap map[string]*Topic

// NewTopicAtomicMap returns a new initialized TopicAtomicMap
func NewTopicAtomicMap() *TopicAtomicMap {
	am := &TopicAtomicMap{}
	am.val.Store(make(_TopicMap, 0))
	return am
}

// Get returns a pointer to Topic for a given key
func (am *TopicAtomicMap) Get(key string) (value *Topic, ok bool) {
	value, ok = am.val.Load().(_TopicMap)[key]
	return value, ok
}

// GetAll returns the underlying map of pointers to Topic
// this map must NOT be modified, to change the map safely use the Set and Delete
// functions and Get the value again
func (am *TopicAtomicMap) GetAll() map[string]*Topic {
	return am.val.Load().(_TopicMap)
}

// Len returns the number of elements in the map
func (am *TopicAtomicMap) Len() int {
	return len(am.val.Load().(_TopicMap))
}

// Set inserts in the map a pointer to Topic under a given key
func (am *TopicAtomicMap) Set(key string, value *Topic) {
	am.mu.Lock()
	defer am.mu.Unlock()

	m1 := am.val.Load().(_TopicMap)
	m2 := make(_TopicMap, len(m1)+1)
	for k, v := range m1 {
		m2[k] = v
	}

	m2[key] = value
	am.val.Store(m2)
	return
}

// Delete removes the pointer to Topic under key from the map
func (am *TopicAtomicMap) Delete(key string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	m1 := am.val.Load().(_TopicMap)
	_, ok := m1[key]
	if !ok {
		return
	}

	m2 := make(_TopicMap, len(m1)-1)
	for k, v := range m1 {
		if k != key {
			m2[k] = v
		}
	}

	am.val.Store(m2)
	return
}
