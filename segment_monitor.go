// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"log"
	"time"
)

// SegmentMonitor periodically checks for segments
// to split or discard at a given interval.
type SegmentMonitor struct {
	nl *NetLog
}

func (sm *SegmentMonitor) start(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		log.Printf("trace: running segment monitor")
		for name, t := range sm.nl.topics.GetAll() {
			sm.check(name, t)
		}
	}
}

func (sm *SegmentMonitor) check(name string, t *Topic) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("alert: segment check failed: %s", err)
		}
	}()

	if t == nil {
		log.Printf("alert: nil topic detected for %q", name)
		return
	}

	err := t.CheckSegments()
	if err != nil {
		log.Printf("error: check segments failed %s", err)
	}
}
