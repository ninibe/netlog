// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"sync"
	"testing"

	"github.com/ninibe/bigduration"
)

func TestMessageBuffer(t *testing.T) {
	t.Parallel()

	// TODO add CompressionNone when supported
	comps := []CompressionType{
		CompressionGzip,
		CompressionSnappy,
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
			iErr := CheckMessageIntegrity(batch, batchSize)
			if iErr != nil {
				t.Errorf("Integrity error on buffered message: %+v", iErr)
			}
		}
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
