// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// ErrInvalidIndexReader is returned on read with nil pointers
var ErrInvalidIndexReader = errors.New("biglog: invalid reader - use NewIndexReader")

// ErrNeedMoreBytes is returned by in the index reader when a single entry does not fit the requested byte limit
// usually the client should double the size when possible and request again
var ErrNeedMoreBytes = errors.New("biglog: maxBytes too low for any entry")

// ErrNeedMoreOffsets is returned by in the index reader when a single entry does not fit the requested offset limit
// usually the client should double the size when possible and request again
var ErrNeedMoreOffsets = errors.New("biglog: maxOffsets too low for any entry")

// IndexReader keeps the state among separate concurrent reads
// IndexReaders handle segment transitions transparently
type IndexReader struct {
	mu  sync.Mutex
	iFO uint32 // file offset of the reader over the index
	bl  *BigLog
	seg *segment
}

// IndexSection holds information about a set of entries of an index
// this information is often used to "drive" a data reader, since this is more performant
// than getting a set of entries.
type IndexSection struct {
	// Offset where the section begins, corresponds with the offset of the first message
	Offset int64
	// ODelta is the number of offsets held by the section
	ODelta int64
	// EDelta is the number of index entries held by the section
	EDelta int64
	// Size is the size of the data mapped by this index section
	Size int64
}

// Entry n holds information about one single entry in the index
type Entry struct {
	// Timestamp of the entry
	Timestamp time.Time
	// Offset corresponds with the offset of the first message
	Offset int64
	// ODelta is the number of offsets held by this entry
	ODelta int
	// Size is the size of the data mapped by this entry
	Size int
}

// NewIndexReader returns an IndexReader that will start reading from a given offset
func NewIndexReader(bl *BigLog, from int64) (r *IndexReader, ret int64, err error) {
	seg, RO, err := bl.locateOffset(from)
	if err != nil {
		return nil, 0, err
	}

	ret = from
	l, err := seg.Lookup(RO)
	if err == ErrEmbeddedOffset {
		ret = from - int64(RO-l.fRO)
	} else if err != nil {
		return nil, 0, err
	}

	r = &IndexReader{
		iFO: l.iFO,
		bl:  bl,
	}

	r.setSegment(seg)
	bl.addReader(r)
	return r, ret, err
}

// ReadEntries reads n entries from the index. This method is useful when scanning single entries one by one
// for streaming use, ReadSection is recommended
func (r *IndexReader) ReadEntries(n int) (entries []*Entry, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r == nil || r.seg == nil {
		return nil, ErrInvalidIndexReader
	}

	entries = make([]*Entry, 0, n)

	for i := 0; i < n; i++ {
		err = r.jumpSeg()
		if err != nil {
			break
		}

		RO, TS, dFO := readEntry(r.seg.index[r.iFO:])
		NRO, _, NdFO := readEntry(r.seg.index[r.iFO+iw:])

		entries = append(entries, &Entry{
			Timestamp: time.Unix(int64(TS), 0),
			Offset:    absolute(RO, r.seg.baseOffset),
			ODelta:    int(NRO - RO),
			Size:      int(NdFO - dFO),
		})

		// advance on index
		r.iFO += iw
	}

	return entries, err
}

// ReadSection reads the section of the index that contains a maximum of offsets or bytes
// it's lower precision than ReadEntries but better suited for streaming since it does not need to allocate
// an Entry struct for every entry read in the index.
func (r *IndexReader) ReadSection(maxOffsets, maxBytes int64) (is *IndexSection, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r == nil || r.seg == nil {
		return nil, ErrInvalidIndexReader
	}

	is = &IndexSection{}

	var firstIter = true
	for {

		err = r.jumpSeg()
		if err != nil {
			break
		}

		RO, _, dFO := readEntry(r.seg.index[r.iFO:])
		NRO, _, NdFO := readEntry(r.seg.index[r.iFO+iw:])

		// check offset limit
		if is.ODelta+int64(NRO-RO) > maxOffsets {
			if firstIter {
				return nil, ErrNeedMoreOffsets
			}

			break
		}

		// check byte limit
		if is.Size+NdFO-dFO > maxBytes {
			if firstIter {
				return nil, ErrNeedMoreBytes
			}

			break
		}

		if firstIter {
			firstIter = false
			is.Offset = absolute(RO, r.seg.baseOffset)
		}

		is.EDelta++
		is.ODelta += int64(NRO - RO)
		is.Size += NdFO - dFO

		// advance on index
		r.iFO += iw
	}

	return is, err
}

// Jump segment if we are at the end of the current one
func (r *IndexReader) jumpSeg() error {
	if r.iFO < atomic.LoadUint32(&r.seg.NiFO) {
		return nil
	}

	seg := r.nextSeg()
	if seg == nil {
		return io.EOF
	}

	r.setSegment(seg)
	r.iFO = 0

	return nil
}

// we need to scan all segments again since the slice could have changed since the last read
func (r *IndexReader) nextSeg() (seg *segment) {

	segs := r.bl.segments()
	i := indexOfSegment(segs, r.seg.baseOffset)
	if i < 0 || i == len(segs)-1 {
		return nil
	}

	return segs[i+1]
}

// Close frees up the segments and renders the reader unusable
// returns nil error to satisfy io.Closer
func (r *IndexReader) Close() error {
	r.bl.removeReader(r)
	atomic.AddInt32(r.seg.readers, -1)
	r = nil
	return nil
}

// Head returns the current offset position of the reader
func (r *IndexReader) Head() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.head()
}

func (r *IndexReader) head() int64 {
	RO, _, _ := readEntry(r.seg.index[r.iFO:])
	return absolute(RO, r.seg.baseOffset)
}

// Seek implements the io.Seeker interface for an index reader.
func (r *IndexReader) Seek(offset int64, whence int) (ret int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch whence {
	case 0:
	case 1:
		offset += r.head()
	case 2:
		offset += r.bl.Latest()
	default:
		panic(fmt.Sprintf("unsupported index reader whence = %d", whence))
	}

	if r == nil || r.seg == nil {
		return -1, ErrInvalidReader
	}

	seg, RO, err := r.bl.locateOffset(offset)
	if err != nil {
		return -1, err
	}

	ret = offset
	l, err := seg.Lookup(RO)
	if err == ErrEmbeddedOffset {
		ret = offset - int64(RO-l.fRO)
	} else if err != nil {
		return -1, err
	}

	r.setSegment(seg)
	r.iFO = l.iFO

	return ret, err
}

func (r *IndexReader) setSegment(seg *segment) {
	if r.seg != nil {
		atomic.AddInt32(r.seg.readers, -1)
	}

	atomic.AddInt32(seg.readers, 1)
	r.seg = seg
}
