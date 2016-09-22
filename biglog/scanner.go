// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"errors"
	"io"
)

// Scanner facilitates reading a BigLog's content one index entry at a time.
// Instantiate always via NewScanner. A scanner is NOT thread-safe, to use
// it concurrently your application must protect it with a mutex.
type Scanner struct {
	token      []byte
	buf        []byte
	maxBufSize int
	entries    []*Entry
	entry      *Entry
	err        error
	start      int
	end        int
	r          *Reader
	ir         *IndexReader
}

// ScannerOption is the type of function used to set internal scanner parameters
type ScannerOption func(*Scanner)

// UseBuffer option facilitates to bring your own buffer to the scanner,
// if the buffer is not big enough for an entry it will be replaced
// with a bigger one. You can prevent this behaviours by setting
// MaxBufferSize to len(buf).
func UseBuffer(buf []byte) ScannerOption {
	return func(s *Scanner) {
		s.buf = buf
	}
}

// MaxBufferSize option defines the maximum buffer size that the
// scanner is allowed to use. If an entry is larger than the max buffer
// scanning will fail with error ErrTooLong. Default size 0 means no limit.
func MaxBufferSize(size int) ScannerOption {
	return func(s *Scanner) {
		s.maxBufSize = size
	}
}

// ErrEntryTooLong is returned when the entry is too big to fit in the allowed buffer size.
var ErrEntryTooLong = errors.New("biglog.Scanner: entry too long")

// ErrInvalidScanner is returned when using an uninitialized scanner.
var ErrInvalidScanner = errors.New("biglog: invalid reader - use NewScanner")

// NewScanner returns a new Scanner starting at `from` offset.
func NewScanner(bl *BigLog, from int64, opts ...ScannerOption) (s *Scanner, err error) {

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

	s = &Scanner{
		r:  r,
		ir: ir,
	}

	for _, opt := range opts {
		opt(s)
	}

	// Initial buffer if none
	if s.buf == nil {
		s.buf = make([]byte, 64*1024)
	}

	if embedded {
		return s, ErrEmbeddedOffset
	}

	return s, nil
}

func (s *Scanner) loadEntries() error {
	entries, err := s.ir.ReadEntries(2)
	s.entries = entries
	if err != nil && err != io.EOF {
		return err
	}

	if len(entries) == 0 && err != io.EOF {
		panic("no entries without known error")
	}

	return nil
}

// Scan advances the Scanner to the next entry, returning false if there is nothing else to read.
func (s *Scanner) Scan() bool {

	if s == nil || s.r == nil {
		s.err = ErrInvalidScanner
		return false
	}

	// Get entries from the index if we have none
	if s.entries == nil || len(s.entries) == 0 {
		err := s.loadEntries()
		s.err = err
		if err != nil || len(s.entries) == 0 {
			return false
		}
	}

	for {
		// if there is data to process
		if s.end > s.start || s.err != nil {
			token, entry := s.extract(s.buf[s.start:s.end])
			s.entry = entry
			s.token = token
			if s.entry != nil {
				s.start += s.entry.Size
			}

			if s.token != nil {
				break
			}
		}

		// Shift data to beginning of buffer if there's lots of empty space or space is needed.
		if s.start > 0 && (s.end == len(s.buf) || s.start > len(s.buf)/2) {
			// copy data
			copy(s.buf, s.buf[s.start:s.end])
			// adjust marks
			s.end -= s.start
			s.start = 0
		}

		// Increase buffer size if full
		if s.end == len(s.buf) {

			// We are already at the limit
			if s.maxBufSize > 0 && len(s.buf) >= s.maxBufSize {
				s.err = ErrEntryTooLong
				return false
			}

			// Try to double the size
			var newSize = len(s.buf) * 2
			// If that's too much, settle for the allowed
			if s.maxBufSize > 0 && newSize > s.maxBufSize {
				newSize = s.maxBufSize
			}

			// create new buffer
			biggerBuf := make([]byte, newSize)
			// copy old data to new buffer
			copy(biggerBuf, s.buf[s.start:s.end])

			// adjust values
			s.buf = biggerBuf
			s.end -= s.start
			s.start = 0
		}

		n, err := s.r.Read(s.buf[s.end:len(s.buf)])
		s.end += n
		s.err = err
		if n == 0 {
			return false
		}
	}

	return true
}

func (s *Scanner) extract(data []byte) (token []byte, entry *Entry) {
	// ask for more data
	if s.entries[0].Size > len(data) {
		return nil, nil
	}

	entry = s.entries[0]
	token = data[0:entry.Size]
	s.entries = s.entries[1:]

	return token, entry
}

// Bytes returns content of the scanned entry.
func (s *Scanner) Bytes() []byte {
	return s.token
}

// Offset returns the initial offset of the scanned entry.
func (s *Scanner) Offset() int64 {
	if s.entry == nil {
		return 0
	}

	return s.entry.Offset
}

// ODelta returns the number of offsets included in the scanned entry.
func (s *Scanner) ODelta() int {
	if s.entry == nil {
		return 0
	}
	return s.entry.ODelta
}

// Err returns the first non-EOF error that was encountered by the Scanner.
func (s *Scanner) Err() error {
	if s.err == io.EOF {
		return nil
	}

	return s.err
}

// Close implements io.Closer and closes the scanner rendering it unusable.
func (s *Scanner) Close() error {
	if err := s.r.Close(); err != nil {
		return err
	}

	return s.ir.Close()
}
