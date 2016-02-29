// generated file - DO NOT EDIT
// command: atomicmapper -pointer -type netlog/biglog.Streamer

package netlog

import (
	"sync"
	"sync/atomic"

	"github.com/ninibe/netlog/biglog"
)

// StreamerAtomicMap is a copy-on-write thread-safe map of pointers to Streamer
type StreamerAtomicMap struct {
	mu  sync.Mutex
	val atomic.Value
}

type _StreamerMap map[string]*biglog.Streamer

// NewStreamerAtomicMap returns a new initialized StreamerAtomicMap
func NewStreamerAtomicMap() *StreamerAtomicMap {
	am := &StreamerAtomicMap{}
	am.val.Store(make(_StreamerMap, 0))
	return am
}

// Get returns a pointer to Streamer for a given key
func (am *StreamerAtomicMap) Get(key string) (value *biglog.Streamer, ok bool) {
	value, ok = am.val.Load().(_StreamerMap)[key]
	return value, ok
}

// GetAll returns the underlying map of pointers to Streamer
// this map must NOT be modified, to change the map safely use the Set and Delete
// functions and Get the value again
func (am *StreamerAtomicMap) GetAll() map[string]*biglog.Streamer {
	return am.val.Load().(_StreamerMap)
}

// Len returns the number of elements in the map
func (am *StreamerAtomicMap) Len() int {
	return len(am.val.Load().(_StreamerMap))
}

// Set inserts in the map a pointer to Streamer under a given key
func (am *StreamerAtomicMap) Set(key string, value *biglog.Streamer) {
	am.mu.Lock()
	defer am.mu.Unlock()

	m1 := am.val.Load().(_StreamerMap)
	m2 := make(_StreamerMap, len(m1)+1)
	for k, v := range m1 {
		m2[k] = v
	}

	m2[key] = value
	am.val.Store(m2)
	return
}

// Delete removes the pointer to Streamer under key from the map
func (am *StreamerAtomicMap) Delete(key string) {
	am.mu.Lock()
	defer am.mu.Unlock()

	m1 := am.val.Load().(_StreamerMap)
	_, ok := m1[key]
	if !ok {
		return
	}

	m2 := make(_StreamerMap, len(m1)-1)
	for k, v := range m1 {
		if k != key {
			m2[k] = v
		}
	}

	am.val.Store(m2)
	return
}
