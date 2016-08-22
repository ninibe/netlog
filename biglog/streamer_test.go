// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"io/ioutil"
	"testing"
)

func TestStreamer(t *testing.T) {
	var entrySize = 1000

	bl := setupData(entrySize)
	defer logDelete(bl, true)
	streamer, err := NewStreamer(bl, 22)
	if err != ErrEmbeddedOffset {
		t.Error(err)
	}

	_, err = streamer.Get(5, int64(entrySize))
	if err != ErrNeedMoreOffsets {
		t.Error(err)
	}

	_, err = streamer.Get(10, int64(entrySize-1))
	if err != ErrNeedMoreBytes {
		t.Error(err)
	}

	delta, err := streamer.Get(10, int64(entrySize+20))
	if err != nil {
		t.Error(err)
	}

	if delta.Offset() != 21 {
		t.Errorf("offset %d instead of %d", delta.Offset(), 21)
	}

	if delta.EntryDelta() != 1 {
		t.Errorf("entry delta %d instead of %d", delta.EntryDelta(), 1)
	}

	if delta.OffsetDelta() != 7 {
		t.Errorf("offset delta %d instead of %d", delta.OffsetDelta(), 7)
	}

	if delta.Size() != int64(entrySize) {
		t.Errorf("delta size of %d instead of %d", delta.Size(), entrySize)
	}

	data, err := ioutil.ReadAll(delta)
	if err != nil {
		t.Error(err)
	}

	if len(data) != entrySize {
		t.Errorf("delta data size of %d instead of %d", cap(data), entrySize)
	}

	err = streamer.Put(delta)
	if err != nil {
		t.Error(err)
	}

	delta, err = streamer.Get(10, int64(entrySize+20))
	if err != nil {
		t.Error(err)
	}

	if delta.Offset() != 28 {
		t.Errorf("offset %d instead of %d", delta.Offset(), 28)
	}

	if delta.EntryDelta() != 1 {
		t.Errorf("entry delta %d instead of %d", delta.EntryDelta(), 1)
	}

	if delta.OffsetDelta() != 8 {
		t.Errorf("offset delta %d instead of %d", delta.OffsetDelta(), 8)
	}

	if delta.Size() != int64(entrySize) {
		t.Errorf("delta size of %d instead of %d", delta.Size(), entrySize)
	}

	data, err = ioutil.ReadAll(delta)
	if err != nil {
		t.Error(err)
	}

	if len(data) != entrySize {
		t.Errorf("delta data size of %d instead of %d", cap(data), entrySize)
	}

}
