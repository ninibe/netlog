// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"io"
	"sync"
)

// Streamer reads the BigLog in Streams deltas which underneath are
// just chunks of the data file with some metadata. Always instantiate
// with NewScanner. See Get and Put methods for usage details.
type Streamer struct {
	mu sync.Mutex
	r  *Reader
	ir *IndexReader
	bl *BigLog
}

// StreamDelta holds a chunk of data from the BigLog. Metadata can be
// inspected with the associated methods. StreamDelta implements the
// io.Reader interface to access the stored data.
type StreamDelta struct {
	offset      int64
	offsetDelta int64
	entryDelta  int64
	size        int64
	sent        int64
	reader      io.Reader
	s           *Streamer
}

// Offset returns the first message to be streamed
func (d *StreamDelta) Offset() int64 { return d.offset }

// OffsetDelta returns the number of offsets to be streamed
func (d *StreamDelta) OffsetDelta() int64 { return d.offsetDelta }

// EntryDelta returns the number of index entries to be streamed
func (d *StreamDelta) EntryDelta() int64 { return d.entryDelta }

// Size returns the number of bytes to be streamed
func (d *StreamDelta) Size() int64 { return d.size }

// Reader implements the io.Reader interface for this delta
func (d *StreamDelta) Read(p []byte) (n int, err error) {
	n, err = d.reader.Read(p)
	d.sent += int64(n)
	return n, err
}

// NewStreamer returns a new Streamer starting at `from` offset.
func NewStreamer(bl *BigLog, from int64) (s *Streamer, err error) {

	var embedded bool
	r, off, err := NewReader(bl, from)
	if err == ErrEmbeddedOffset {
		embedded = true
	} else if err != nil {
		return nil, err
	}

	ir, _, err := NewIndexReader(bl, off)
	if err != nil {
		return nil, err
	}

	ds := &Streamer{
		r:  r,
		ir: ir,
		bl: bl,
	}

	if embedded {
		return ds, ErrEmbeddedOffset
	}

	return ds, nil
}

// Get given a maximum number of offsets and a maximum number of bytes, returns a StreamDelta
// for the biggest chunk that satisfied the limits until EOF. If the next entry is too big
// for either limit, either ErrNeedMoreOffsets or ErrNeedMoreBytes is returned.
// IMPORTANT: The StreamDelta must be "Put" back before a new one can issued.
func (st *Streamer) Get(maxOffsets, maxBytes int64) (delta *StreamDelta, err error) {
	st.mu.Lock()

	sec, err := st.ir.ReadSection(maxOffsets, maxBytes)
	if err != nil {
		st.mu.Unlock()
		return nil, err
	}

	delta = &StreamDelta{
		offset:      sec.Offset,
		offsetDelta: sec.ODelta,
		entryDelta:  sec.EDelta,
		size:        sec.Size,
		reader:      io.LimitReader(st.r, sec.Size),
		s:           st,
	}

	return delta, err
}

// Put must be called once StreamDelta has been successfully read
// so the reader can advance and a new StreamDelta can be issued.
func (st *Streamer) Put(delta *StreamDelta) (err error) {
	defer st.mu.Unlock()
	return nil
}

func (st *Streamer) correctPartialDelta(delta *StreamDelta) error {
	// TODO implement when defined.
	Logger.Print("error: correctPartialDelta not implemented")
	return nil
}
