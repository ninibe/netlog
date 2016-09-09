// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/net/context"

	"github.com/ninibe/netlog/biglog"
)

// file name structure for persisted scanners
var scannerPattern = "%36s.scanner"

// TopicScanner reads one by one over the messages in a topic
// blocking until new data is available for a period of time.
// TopicScanners are thread-safe.
type TopicScanner interface {
	ID() string
	Scan(ctx context.Context) (m Message, offset int64, err error)
	Info() TScannerInfo
	Close() error
}

// NewTopicScanner returns a new topic scanner ready to scan starting at offset `from`,
// if persist is true, the scanner and it's last position will survive across server restarts
func NewTopicScanner(t *Topic, ID string, from int64, persist bool) (TopicScanner, error) {
	bts, err := newBLTopicScanner(t, ID, from)
	if err != nil {
		return nil, ExtErr(err)
	}

	if !persist {
		return bts, nil
	}

	return newPersistentTopicScanner(t, bts)
}

// BLTopicScanner implements TopicScanner reading from BigLog.
type BLTopicScanner struct {
	mu       sync.RWMutex
	_ID      string
	topic    *Topic
	from     int64
	last     int64
	messages []Message

	sc *biglog.Scanner
	wc *biglog.Watcher
}

// NewBLTopicScanner returns a new topic scanner ready to scan starting at offset `from`
func newBLTopicScanner(t *Topic, ID string, from int64) (bts *BLTopicScanner, err error) {
	sc, err := biglog.NewScanner(t.bl, from)
	if err != nil && err != biglog.ErrEmbeddedOffset {
		return nil, err
	}

	bts = &BLTopicScanner{
		_ID:   ID,
		topic: t,
		from:  from,
		last:  -1,
		sc:    sc,
		wc:    biglog.NewWatcher(t.bl),
	}

	// auto-scan forward if embedded offset
	if err == biglog.ErrEmbeddedOffset {
		err = bts.scanForward(from)
	}

	return bts, err
}

// scans in a loop until the next offset is the target offset
func (ts *BLTopicScanner) scanForward(target int64) (err error) {
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

// Scan advances the Scanner to the next message, returning the message and the offset.
// Scan will block when it reaches EOF until there is more data available,
// the user must provide a context to cancel the request when it needs to stop waiting.
func (ts *BLTopicScanner) Scan(ctx context.Context) (m Message, offset int64, err error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	for {
		// if there is a buffered message
		//  from a set return one of those
		if len(ts.messages) > 0 {
			m = ts.messages[0]
			offset = ts.last

			ts.last++
			ts.messages = ts.messages[1:]

			return m, offset, nil
		}

		// scan a new entry
		ok := ts.scan(ctx)
		if ok {
			ts.last = ts.sc.Offset()

			// if it's got only one message return it
			if ts.sc.ODelta() == 1 {
				return Message(ts.sc.Bytes()), ts.last, nil
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

func (ts *BLTopicScanner) scan(ctx context.Context) bool {
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

// ID returns the ID of the scanner
func (ts *BLTopicScanner) ID() string {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return ts._ID
}

// Close implements io.Closer and releases the TopicScanner resources.
func (ts *BLTopicScanner) Close() error {
	if err := ts.sc.Close(); err != nil {
		return err
	}

	return ts.wc.Close()
}

// TScannerInfo holds the scanner's offset information
type TScannerInfo struct {
	ID      string `json:"id"`
	Next    int64  `json:"next"`
	From    int64  `json:"from"`
	Persist bool   `json:"persistent"`
}

// Info returns a TScannerInfo struct with the scanner's
// next offset and the initial offset.
func (ts *BLTopicScanner) Info() TScannerInfo {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	return TScannerInfo{
		ID:   ts._ID,
		Next: ts.next(),
		From: ts.from,
	}
}

// next returns next index for the scanner
func (ts *BLTopicScanner) next() (next int64) {
	if ts.last < 0 {
		next = ts.from
	} else {
		next = ts.last + 1
	}

	oldest := ts.topic.bl.Oldest()
	if oldest > next {
		next = oldest
	}

	return
}

// newPersistentTopicScanner returns a new topic scanner wrapper which will persist the scanners state
func newPersistentTopicScanner(t *Topic, ts TopicScanner) (*PersistentTopicScanner, error) {

	err := os.MkdirAll(t.readersDir(), 0755)
	if err != nil {
		log.Printf("error: %s", err)
		return nil, ErrInvalidDir
	}

	fname := fmt.Sprintf(scannerPattern, ts.ID())
	fpath := filepath.Join(t.readersDir(), fname)

	f, err := os.OpenFile(fpath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		log.Printf("error: %s", err)
		return nil, ErrInvalidDir
	}

	pts := &PersistentTopicScanner{
		f:     f,
		fpath: fpath,
		ts:    ts,
		oc:    make(chan int64, 100),
	}

	go pts.persist()
	return pts, nil
}

// PersistentTopicScanner synchronizes the underlying
// scanner state to a given writer
type PersistentTopicScanner struct {
	f     *os.File
	fpath string
	ts    TopicScanner
	oc    chan int64
}

// ID the ID of the scanner
func (p *PersistentTopicScanner) ID() string {
	return p.ts.ID()
}

// Scan offloads the actual scan to the underlying scanner while updates the last read offset
func (p *PersistentTopicScanner) Scan(ctx context.Context) (m Message, offset int64, err error) {
	m, offset, err = p.ts.Scan(ctx)
	if offset < 0 {
		return m, offset, err
	}

	select {
	case p.oc <- offset:
	default:
	}

	return m, offset, err
}

// Info returns a TScannerInfo struct with the scanner's
// next offset and the last scanned one
func (p *PersistentTopicScanner) Info() TScannerInfo {
	i := p.ts.Info()
	i.Persist = true
	return i
}

// Close deletes the offset tracking file, closes the
// offset channel and closes the underlying scanner
func (p *PersistentTopicScanner) Close() error {
	err := os.Remove(p.fpath)
	if err != nil {
		log.Printf("error: can't remove %s: %s", p.fpath, err)
		return err
	}

	close(p.oc)
	return p.ts.Close()
}

func (p *PersistentTopicScanner) persist() {
	buf := make([]byte, 8)
	for o := range p.oc {
		enc.PutUint64(buf, uint64(o))
		_, err := p.f.WriteAt(buf, 0)
		if err != nil {
			log.Printf("error: failed to persist topic scanner %s: %s", p.ts.Info().ID, err)
		}
	}
}
