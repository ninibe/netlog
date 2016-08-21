// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"bytes"
	"io"
	"testing"
)

func TestReader(t *testing.T) {
	var readerTestDataSize = 99

	bl := setupData(readerTestDataSize)
	defer logDelete(bl, true)

	// TODO make this loop all cases
	// Test seek embedded offset
	r, ret, err := NewReader(bl, 12)
	if err != ErrEmbeddedOffset {
		t.Error(err)
	}

	if ret != 10 {
		t.Errorf("Expected reader to start on %d instead of %d", 10, ret)
	}

	single := make([]byte, readerTestDataSize)

	n, err := r.Read(single)
	if err != nil {
		t.Error(err)
	}

	if n != readerTestDataSize {
		t.Errorf("read %d bytes instead of %d", n, readerTestDataSize)
	}

	if err := r.Close(); err != nil {
		t.Error(err)
	}

	double := bytes.Repeat(single, 2)
	doubuf := make([]byte, readerTestDataSize*2)

	r, _, err = NewReader(bl, 0)
	if err != nil {
		t.Error(err)
	}

	// read 4 chucks (8 entries)
	for i := 1; i < 5; i++ {
		n, err := r.Read(doubuf)
		if err != nil {
			t.Error(err)
		}

		if n != readerTestDataSize*2 {
			t.Errorf("read %d bytes instead of %d", n, readerTestDataSize*2)
		}

		if !bytes.Equal(double, doubuf) {
			t.Errorf("wrong bytes in iteration i=%d read\n Expected: % x\n Actual  : % x\n", i, double, doubuf)
		}
	}

	// read last odd entry
	n, err = r.Read(doubuf)
	if err != io.EOF {
		t.Error(err)
	}

	if n != readerTestDataSize {
		t.Errorf("read %d bytes instead of %d", n, readerTestDataSize)
	}

	if !bytes.Equal(single, doubuf[:n]) {
		t.Errorf("wrong bytes in final iteration read\n Expected: % x\n Actual  : % x\n", single, doubuf[:n])
	}

	// Seek back to embedded offset
	ret, err = r.Seek(12, 0)
	if err != ErrEmbeddedOffset {
		t.Error(err)
	}

	if ret != 10 {
		t.Errorf("Expected reader to start on %d instead of %d", 10, ret)
	}

	seekSingle := make([]byte, readerTestDataSize)

	n, err = r.Read(seekSingle)
	if err != nil {
		t.Error(err)
	}

	if n != readerTestDataSize {
		t.Errorf("read %d bytes instead of %d", n, readerTestDataSize)
	}

	if !bytes.Equal(single, seekSingle) {
		t.Errorf("wrong bytes reading single entry after seek\n Expected: % x\n Actual  : % x\n", single, seekSingle)
	}
}

func TestReaderInterfaces(t *testing.T) {
	bl := setupData(100)
	defer logDelete(bl, true)

	r, _, err := NewReader(bl, 0)
	if err != nil {
		t.Fatal(err)
	}

	var reader io.Reader
	var closer io.Closer
	var seeker io.Seeker

	reader = r
	closer = r
	seeker = r

	_ = reader
	_ = closer
	_ = seeker
}
