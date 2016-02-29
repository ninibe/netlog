// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"sync"

	"golang.org/x/net/context"

	"github.com/ninibe/netlog/biglog"
)

// TopicScanner reads one by one over the messages in a topic
// blocking until new data is available for a period of time.
type TopicScanner struct {
	ID string
	mu sync.RWMutex
	sc *biglog.Scanner
	wc *biglog.Watcher
}

// NewTopicScanner returns a new topic scanner on a given BigLog from a given 'from' offset.
func NewTopicScanner(bl *biglog.BigLog, from int64) (ts *TopicScanner, err error) {
	if bl == nil {
		return nil, biglog.ErrInvalid
	}

	sc, err := biglog.NewScanner(bl, from)
	return &TopicScanner{
		sc: sc,
		wc: biglog.NewWatcher(bl),
	}, err
}

// Scan advances the Scanner to the next entry, returning the bytes, the offset number and the
// number of offsets within the entry. Scan will block when it reaches EOF until there is more
// data available, the user must provide a context to cancel the request when it needs to stop waiting.
func (ts *TopicScanner) Scan(ctx context.Context) (data []byte, offset int64, delta int, err error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ok := ts.scan(ctx); ok {
		return ts.Bytes(), ts.Offset(), ts.sc.ODelta(), nil
	}

	if ts.sc.Err() != nil {
		return nil, 0, 0, ts.sc.Err()
	}

	return nil, 0, 0, ErrEndOfTopic
}

// TODO handle message batches
func (ts *TopicScanner) scan(ctx context.Context) bool {
	for {
		ok := ts.sc.Scan()

		select {
		case <-ctx.Done():
			return ok
		default:
		}

		// we've got data?
		if ok || ts.Err() != nil {
			return ok
		}

		// block until done or new data
		select {
		case <-ctx.Done():
			return false
		case <-ts.wc.Watch():
			continue
		}
	}
}

// Bytes returns payload of the scanned entry.
func (ts *TopicScanner) Bytes() []byte {
	// TODO unpack embedded?
	m := Message(ts.sc.Bytes())
	return m.Payload()
}

// Offset returns the initial offset of the scanned entry.
func (ts *TopicScanner) Offset() int64 { return ts.sc.Offset() } // move to return param

// Err returns the first non-EOF error that was encountered by the Scanner.
func (ts *TopicScanner) Err() error { return ts.sc.Err() }

// Close implements io.Closer and releases the TopicScanner resources.
func (ts *TopicScanner) Close() error {
	if err := ts.sc.Close(); err != nil {
		return err
	}

	return ts.wc.Close()
}
