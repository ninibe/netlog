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
)

// ErrInvalidReader is returned on read with nil pointers
var ErrInvalidReader = errors.New("biglog: invalid reader - use NewReader")

// Reader keeps the state among separate concurrent reads
// Readers handle segment transitions transparently
type Reader struct {
	mu  sync.Mutex
	bl  *BigLog
	seg *segment
	dFO int64
}

// NewReader returns a Reader that will start reading from a given offset
// the reader implements the io.ReaderCloser interface
func NewReader(bl *BigLog, from int64) (r *Reader, ret int64, err error) {
	seg, RO, err := bl.locateOffset(from)
	if err != nil {
		return nil, -1, err
	}

	ret = from
	l, err := seg.Lookup(RO)
	if err == ErrEmbeddedOffset {
		ret = from - int64(RO-l.fRO)
	} else if err != nil {
		return nil, -1, err
	}

	r = &Reader{
		dFO: l.dFO,
		bl:  bl,
	}

	r.setSegment(seg)
	bl.addReader(r)

	return r, ret, err
}

// Read reads bytes from the big log, offset is auto-incremented in every read
func (r *Reader) Read(b []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r == nil || r.seg == nil {
		return 0, ErrInvalidReader
	}

	var sumn int
	var seg *segment

	for {
		n, err = r.seg.ReadAt(b[sumn:], r.dFO)
		r.dFO += int64(n)
		if err != io.EOF {
			break
		}

		seg = r.nextSeg()
		if seg == nil {
			err = io.EOF
			break
		}

		r.setSegment(seg)
		r.dFO = 0
		sumn += n
	}

	n += sumn

	return n, err
}

// we need to scan all segments every time since
// the slice could have changed since the last read
func (r *Reader) nextSeg() (seg *segment) {

	segs := r.bl.segments()
	i := indexOfSegment(segs, r.seg.baseOffset)
	if i < 0 || i == len(segs)-1 {
		return nil
	}

	return segs[i+1]
}

// Close frees up the segments and renders the reader unusable
// returns nil error to satisfy io.Closer
func (r *Reader) Close() error {
	atomic.AddInt32(r.seg.readers, -1)
	r.bl.removeReader(r)
	r = nil
	return nil
}

// Seek implements the io.Seeker interface for a reader.
// Only whence=1 (relative) is not supported since the data reader has no
// knowledge of where it is once it starts reading the biglog.
func (r *Reader) Seek(offset int64, whence int) (ret int64, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch whence {
	case 0:
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
	r.dFO = l.dFO

	return ret, err
}

func (r *Reader) setSegment(seg *segment) {
	if r.seg != nil {
		atomic.AddInt32(r.seg.readers, -1)
	}

	atomic.AddInt32(seg.readers, 1)
	r.seg = seg
}
