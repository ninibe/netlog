// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

// Watcher provides a notification channel
// for changes in a given BigLog
type Watcher struct {
	wc chan struct{}
	bl *BigLog
}

// NewWatcher creates a new Watcher for the provided BigLog.
func NewWatcher(bl *BigLog) (wa *Watcher) {
	wa = &Watcher{
		wc: make(chan struct{}, 1),
		bl: bl,
	}

	bl.addWatcher(wa.wc)
	return wa
}

// Watch returns a channel that gets sent an empty struct when
// there has been changes in the BigLog since the last time
// the channel was read.
func (wa *Watcher) Watch() <-chan struct{} {
	return wa.wc
}

// Close releases the Watcher.
func (wa *Watcher) Close() error {
	wa.bl.removeWatcher(wa.wc)
	close(wa.wc)
	return nil
}
