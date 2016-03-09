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
// TopicScanners are thread-safe.
type TopicScanner struct {
	ID string

	mu       sync.RWMutex
	start    int64
	offset   int64
	messages []Message

	sc *biglog.Scanner
	wc *biglog.Watcher
}

// NewTopicScanner returns a new topic scanner on a given BigLog from a given 'from' offset.
func NewTopicScanner(bl *biglog.BigLog, from int64) (ts *TopicScanner, err error) {
	if bl == nil {
		return nil, biglog.ErrInvalid
	}

	sc, err := biglog.NewScanner(bl, from)

	ts = &TopicScanner{
		start:  from,
		offset: -1,
		sc:     sc,
		wc:     biglog.NewWatcher(bl),
	}

	// auto-scan forward if embedded offset
	if err == biglog.ErrEmbeddedOffset {
		err = ts.scanForward(from)
	}

	return ts, err
}

// scans in a loop until the next offset is the target offset
func (ts *TopicScanner) scanForward(target int64) (err error) {
	ctx := context.Background()
	var offset int64
	for {
		_, offset, err = ts.Scan(ctx)
		// escape when next is the target offset
		if offset+1 == target {
			break
		}

		if err != nil {
			return err
		}
	}

	return err
}

// Scan advances the Scanner to the next message, returning the bytes and the offset.
// Scan will block when it reaches EOF until there is more data available,
// the user must provide a context to cancel the request when it needs to stop waiting.
func (ts *TopicScanner) Scan(ctx context.Context) (data []byte, offset int64, err error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	for {
		// if there is a buffered message
		//  from a set return one of those
		if len(ts.messages) > 0 {
			data = ts.messages[0].Payload()
			offset = ts.offset

			ts.offset++
			ts.messages = ts.messages[1:]

			return data, offset, nil
		}

		// scan a new entry
		ok := ts.scan(ctx)
		if ok {
			ts.offset = ts.sc.Offset()

			// if it's got only one message return payload
			if ts.sc.ODelta() == 1 {
				msg := Message(ts.sc.Bytes())
				return msg.Payload(), ts.offset, nil
			}

			// unpack message-set into buffer
			ts.messages, err = Unpack(ts.sc.Bytes())
		}

		if ts.sc.Err() != nil {
			return nil, -1, ts.sc.Err()
		}

		if !ok {
			break
		}
	}

	return nil, -1, ErrEndOfTopic
}

func (ts *TopicScanner) scan(ctx context.Context) bool {
	for {
		ok := ts.sc.Scan()

		select {
		case <-ctx.Done():
			return ok
		default:
		}

		// we've got data?
		if ok || ts.sc.Err() != nil {
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

// Offset returns the offset of the last scanned message.
func (ts *TopicScanner) Offset() int64 {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return ts.offset
}

// Start returns the offset of the first scanned message.
func (ts *TopicScanner) Start() int64 {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return ts.start
}

// Close implements io.Closer and releases the TopicScanner resources.
func (ts *TopicScanner) Close() error {
	if err := ts.sc.Close(); err != nil {
		return err
	}

	return ts.wc.Close()
}

// TScannerInfo holds the scanner's offset information
type TScannerInfo struct {
	Start int64 `json:"start"`
	Last  int64 `json:"last"`
}

// Info returns a TScannerInfo struct with the scanner's
// original starting offset and the last scanned one
func (ts *TopicScanner) Info() TScannerInfo {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return TScannerInfo{
		Start: ts.start,
		Last:  ts.offset,
	}
}
