// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"sync"
	"testing"
	"time"

	"github.com/ninibe/bigduration"
	"github.com/ninibe/netlog/message"
)

func TestMessageBuffer(t *testing.T) {
	t.Parallel()

	comps := []message.CompressionType{
		message.CompressionGzip,
		message.CompressionSnappy,
		message.CompressionNone,
	}

	for _, comp := range comps {
		batchSize := 5
		w := &testNWriter{}
		bd, _ := bigduration.ParseBigDuration("1h")
		mb := newMessageBuffer(w, TopicSettings{
			BatchInterval:    bd,
			BatchNumMessages: batchSize,
			CompressionType:  comp,
		})

		data := randMessageSet()
		for k := range data {
			_, err := mb.Write(data[k])
			if err != nil {
				t.Errorf("Failed to write to message buffer %s", err)
			}
		}

		expWrites := int(len(data) / batchSize)
		if expWrites != w.writes {
			t.Errorf("Message buffer flushed %d times. Expected %d", expWrites, w.writes)
		}

		expOffsets := expWrites * batchSize
		if w.offsets != expOffsets {
			t.Errorf("Message buffer registered %d offsets. Expected %d", w.offsets, expOffsets)
		}

		for _, batch := range w.data {
			iErr := message.CheckMessageIntegrity(batch, batchSize)
			if iErr != nil {
				t.Errorf("Integrity error on buffered message: %+v", iErr)
			}
		}
	}
}

func TestMessageBufferFlusher(t *testing.T) {
	t.Parallel()

	interval := "500ms"
	bd, _ := bigduration.ParseBigDuration(interval)
	nw := &testNWriter{}

	mb := newMessageBuffer(nw, TopicSettings{
		BatchInterval:    bd,
		BatchNumMessages: 100000, // something unreachable
		CompressionType:  message.CompressionGzip,
	})

	// Give the flusher a head start
	time.Sleep(bd.Duration() / 2)

	batchSize := 5
	for i := 1; i <= batchSize; i++ {
		_, err := mb.Write([]byte("foo"))
		if err != nil {
			t.Errorf("Failed to write to message buffer %s", err)
		}
	}

	if nw.Writes() != 0 {
		t.Error("Message buffer flushed ahead of time")
	}

	// wait for flusher to kick in
	time.Sleep(bd.Duration())

	if nw.Writes() != 1 {
		t.Error("Message buffer not flushed in time")
	}
}

type testNWriter struct {
	mutex   sync.Mutex
	writes  int
	offsets int
	data    [][]byte
}

func (nw *testNWriter) WriteN(p []byte, n int) (written int, err error) {
	nw.mutex.Lock()
	nw.writes++
	nw.offsets += n
	nw.data = append(nw.data, p)
	nw.mutex.Unlock()
	return len(p), nil
}

func (nw *testNWriter) Writes() int {
	nw.mutex.Lock()
	defer nw.mutex.Unlock()

	return nw.writes
}
