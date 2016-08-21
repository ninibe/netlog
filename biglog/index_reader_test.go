// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"io"
	"testing"
)

var indexTestDataSize = 100

func TestReadEntries(t *testing.T) {
	bl := setupData(indexTestDataSize)
	defer logDelete(bl, true)

	// TODO make this loop all cases
	// Test seek embedded offset
	ir, ret, err := NewIndexReader(bl, 12)
	if err != ErrEmbeddedOffset {
		t.Error(err)
	}

	if ret != 10 {
		t.Errorf("Expected reader to start on %d instead of %d", 10, ret)
	}

	entries, err := ir.ReadEntries(1000)
	if err != io.EOF {
		t.Error(err)
	}

	if len(entries) != 5 {
		t.Errorf("Expected %d entries instead of %d", 5, len(entries))
	}

	if err := ir.Close(); err != nil {
		t.Error(err)
	}

	var offset = 0
	ir, _, err = NewIndexReader(bl, 0)
	panicOn(err)
	for i := 1; i < 10; i++ {
		offset += i - 1

		entries, err := ir.ReadEntries(1)
		panicOn(err)
		if len(entries) != 1 {
			t.Fatalf("Expected %d entries instead of %d", 1, len(entries))
		}

		if entries[0].Offset != int64(offset) {
			t.Errorf("Expected offset %d instead of %d", offset, entries[0].Offset)
		}

		if entries[0].Size != indexTestDataSize {
			t.Errorf("Expected size of %d instead of %d", indexTestDataSize, entries[0].Size)
		}
	}

	_, err = ir.ReadEntries(1)
	if err != io.EOF {
		t.Error(err)
	}
}

func TestReadSection(t *testing.T) {
	bl := setupData(indexTestDataSize)
	defer logDelete(bl, true)

	// TODO make this loop all cases
	// Test seek embedded offset
	ir, _, err := NewIndexReader(bl, 21)
	if err != nil {
		t.Error(err)
	}

	_, err = ir.ReadSection(5, 200)
	if err != ErrNeedMoreOffsets {
		t.Error(err)
	}

	_, err = ir.ReadSection(10, 50)
	if err != ErrNeedMoreBytes {
		t.Error(err)
	}

	// fetch [21:27]
	sec, err := ir.ReadSection(10, 500)
	if err != nil {
		t.Error(err)
	}

	if sec.Offset != 21 {
		t.Errorf("offset %d instead of %d", sec.Offset, 21)
	}

	if sec.EDelta != 1 {
		t.Errorf("entry delta %d instead of %d", sec.EDelta, 1)
	}

	if sec.ODelta != 7 {
		t.Errorf("offset delta %d instead of %d", sec.ODelta, 7)
	}

	if sec.Size != int64(indexTestDataSize) {
		t.Errorf("offset delta %d instead of %d", sec.Size, indexTestDataSize)
	}

	if err := ir.Close(); err != nil {
		t.Error(err)
	}

	ir, _, err = NewIndexReader(bl, 0)
	if err != nil {
		t.Error(err)
	}

	// fetch all data
	sec, err = ir.ReadSection(100, 1000)
	if err != io.EOF {
		t.Error(err)
	}

	if sec.Offset != 0 {
		t.Errorf("offset %d instead of %d", sec.Offset, 0)
	}

	if sec.EDelta != 9 {
		t.Errorf("entry delta %d instead of %d", sec.EDelta, 9)
	}

	if sec.ODelta != 45 {
		t.Errorf("offset delta %d instead of %d", sec.ODelta, 45)
	}

	if sec.Size != int64(indexTestDataSize*9) {
		t.Errorf("offset delta %d instead of %d", sec.Size, int64(indexTestDataSize*9))
	}
}

func TestIndexReaderInterfaces(t *testing.T) {
	bl := setupData(100)
	defer logDelete(bl, true)

	r, _, err := NewIndexReader(bl, 0)
	if err != nil {
		t.Fatal(err)
	}

	var closer io.Closer
	var seeker io.Seeker

	closer = r
	seeker = r

	_ = closer
	_ = seeker
}
