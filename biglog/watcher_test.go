// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"testing"
	"time"

	"sync/atomic"
)

func TestNewWatcher(t *testing.T) {
	bl := tempBigLog()

	var events int32
	var closed int32

	wa := NewWatcher(bl)
	go func() {
		for range wa.Watch() {
			atomic.AddInt32(&events, 1)
		}
		atomic.AddInt32(&closed, 1)
	}()

	time.Sleep(time.Millisecond)
	_, err := bl.Write([]byte("first"))
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Millisecond)
	_, err = bl.Write([]byte("second"))
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Millisecond)
	if atomic.LoadInt32(&events) != 2 {
		t.Errorf("Watcher detected %d events instead of %d", atomic.LoadInt32(&events), 2)
	}

	err = wa.Close()
	if err != nil {
		t.Error(err)
	}

	time.Sleep(time.Millisecond)

	if atomic.LoadInt32(&closed) != 1 {
		t.Error("Watcher not closed correctly")
	}
}

func TestWatcherCount(t *testing.T) {
	bl := tempBigLog()
	defer logDelete(bl, true)

	var n = 10
	var nw = make([]*Watcher, n)

	for i := 0; i < n; i++ {
		nw[i] = NewWatcher(bl)
		count := len(bl.watchers.Load().(watcherMap))
		if count != i+1 {
			t.Errorf("Watcher map count error. Expected %d Actual %d", i+1, count)
		}
	}

	for i := 0; i < n; i++ {
		err := nw[i].Close()
		if err != nil {
			t.Error(err)
		}

		count := len(bl.watchers.Load().(watcherMap))
		if count != 9-i {
			t.Errorf("Watcher map count error. Expected %d Actual %d", 9-i, count)
		}
	}
}
