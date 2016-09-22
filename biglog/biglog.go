// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"bufio"
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrEmbeddedOffset is returned when the offset in embedded in a batch and can not be retrieved individually
	ErrEmbeddedOffset = errors.New("biglog: embedded offset")

	// ErrNotFound is returned when the requested offset is not in the log
	ErrNotFound = errors.New("biglog: offset not found")

	// ErrLastSegment is returned trying to delete the only segment in the biglog
	// To delete all segments use BigLog.Delete()
	ErrLastSegment = errors.New("biglog: last segment can't be deleted")

	// ErrInvalid is returned when the big log format is not recognized
	ErrInvalid = errors.New("biglog: invalid biglog")

	// ErrBusy is returned when there are active readers or watchers while trying
	// to close/delete the biglog
	ErrBusy = errors.New("biglog: resource busy")
)

// Option is the type of function used to set internal parameters
type Option func(*BigLog)

// BufioWriter option defines the buffer
// size to use for writing segments
func BufioWriter(size int) Option {
	return func(bl *BigLog) {
		bl.bufioSize = size
	}
}

// BigLog is the main structure TODO ...
type BigLog struct {
	name    string
	dirPath string

	mu        sync.RWMutex
	segs      []*segment
	hotSeg    atomic.Value // the currently active segment
	bufioSize int

	wmu      sync.Mutex
	watchers atomic.Value

	rmu     sync.Mutex
	readers atomic.Value
}

// SetOpts sets options after BigLog has been created
func (bl *BigLog) SetOpts(opts ...Option) {
	for _, opt := range opts {
		opt(bl)
	}
}

type watcherMap map[chan struct{}]struct{}
type readerMap map[io.Closer]struct{}

// Create new biglog
// `dirPath` does not include the name
// `indexSize` represents the number of entries that the index can hold
// current entry size in the index is 4 bytes, so every segment will have
// a preallocated index file of disk size = maxIndexEntries * 4 bytes.
// In the index each write will consumed an entry, independently of how many
// offsets are contained.
func Create(dirPath string, maxIndexEntries int) (*BigLog, error) {
	err := os.Mkdir(dirPath, 0755)
	if err != nil {
		return nil, err
	}

	seg, err := createSegment(dirPath, maxIndexEntries, 0)
	if err != nil {
		return nil, err
	}

	err = seg.Close()
	if err != nil {
		return nil, err
	}

	return Open(dirPath)
}

// Open loads a BigLog from disk by loading all segments from the index files
// inside the given directory. The last segment - ordered by the base offset
// encoded in the file names - is set to be the hot segment of the BigLog.
// The created BigLog is ready to serve any watchers or readers immediately.
//
// ErrInvalid is returned if there are no index files within dirPath.
// ErrLoadSegment is returned if a segment can not be loaded.
func Open(dirPath string) (*BigLog, error) {
	dirfs, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	var indexes []string
	for _, f := range dirfs {
		if filepath.Ext(f.Name()) == ".index" {
			indexes = append(indexes, f.Name())
		}
	}

	if len(indexes) == 0 {
		return nil, ErrInvalid
	}

	dirPath, _ = filepath.Abs(dirPath)
	bl := &BigLog{
		name:    filepath.Base(dirPath),
		dirPath: dirPath,
		segs:    make([]*segment, 0),
	}

	// initialize hot segment type for atomic load
	var hotSeg *segment
	bl.hotSeg.Store(hotSeg)

	// sort by index file name, should reflect base offset
	sort.Sort(sort.StringSlice(indexes))
	var seg *segment
	for _, index := range indexes {
		seg, err = loadSegment(filepath.Join(dirPath, index))
		if err != nil {
			return nil, err
		}

		bl.segs = append(bl.segs, seg)
		hotSeg = seg
	}

	err = hotSeg.healthCheckPartialWrite()
	if err != nil {
		return nil, err
	}

	err = bl.setHotSeg(hotSeg)
	bl.watchers.Store(make(watcherMap, 0))
	bl.readers.Store(make(readerMap, 0))
	go bl.notify()

	return bl, err
}

// segments lists the current log segments
func (bl *BigLog) segments() []*segment {
	bl.mu.RLock()
	var segs = bl.segs
	bl.mu.RUnlock()
	return segs
}

// DirPath returns the absolute path to
// the folder with the BigLog's files
func (bl *BigLog) DirPath() string {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.dirPath
}

// Write writes len(b) bytes from b into the currently active segment of bl as
// a single entry. Splitting up the BigLog into more segments is handled
// transparently if the currently active segment is full.
// It returns the number of bytes written from b (0 <= n <= len(b))
// and any error encountered that caused the write to stop early.
func (bl *BigLog) Write(b []byte) (written int, err error) {
	return bl.WriteN(b, 1)
}

// WriteN writes a batch of n entries from b into the currently active segment.
// Splitting up the BigLog into more segments is handled transparently if
// the currently active segment is full.
// It returns the number of bytes written from b (0 <= n <= len(b))
// and any error encountered that caused the write to stop early.
func (bl *BigLog) WriteN(b []byte, n int) (written int, err error) {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.writeN(b, uint32(n))
}

func (bl *BigLog) writeN(b []byte, n uint32) (written int, err error) {
	err = bl.splitIfFull()
	if err != nil {
		return 0, err
	}

	return bl.hotSeg.Load().(*segment).WriteN(b, n)
}

// ReadFrom reads data from src into the currently active segment until EOF or
// the first error. All read data is indexed as a single entry. Splitting up
// the BigLog into more segments is handled transparently if the currently
// active segment is full.
// It returns the number of bytes written and any error encountered.
func (bl *BigLog) ReadFrom(src io.Reader) (written int64, err error) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	err = bl.splitIfFull()
	if err != nil {
		return 0, err
	}

	return bl.hotSeg.Load().(*segment).ReadFrom(src)
}

// After returns the first offset after a given time.
func (bl *BigLog) After(t time.Time) (int64, error) {
	bl.mu.RLock()
	defer bl.mu.RUnlock()

	seg, RO, err := bl.locateTS(uint32(t.Unix()))
	if err != nil {
		return 0, err
	}

	return absolute(RO, seg.baseOffset), nil
}

// Oldest returns oldest/lowest available offset.
func (bl *BigLog) Oldest() int64 {
	bl.mu.RLock()
	defer bl.mu.RUnlock()
	return bl.segs[0].baseOffset
}

// Latest returns latest/highest available offset.
func (bl *BigLog) Latest() int64 {
	bl.mu.RLock()
	defer bl.mu.RUnlock()
	return bl.latest()
}

// latest returns latest/highest available offset.
func (bl *BigLog) latest() int64 {
	hotSeg := bl.hotSeg.Load().(*segment)

	// If there is only one segment and it's empty
	if len(bl.segs) == 1 && hotSeg.NRO == 1 {
		return -1 // no data
	}

	// latest = next available -1
	return absolute(hotSeg.NRO-1, hotSeg.baseOffset)
}

// Name returns the big log's name, which maps to directory path that contains
// the index and data files.
func (bl *BigLog) Name() string {
	bl.mu.RLock()
	defer bl.mu.RUnlock()
	return bl.name
}

// Split creates a new segment in bl's dirPath starting at the highest
// available offset+1. The new segment has the same size as the old one
// and becomes the new hot (active) segment.
func (bl *BigLog) Split() error {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.split()
}

func (bl *BigLog) split() (err error) {
	maxIndexEntries := int(bl.hotSeg.Load().(*segment).indexSize / iw)
	seg, err := createSegment(bl.dirPath, maxIndexEntries, bl.latest()+1)
	if err != nil {
		return err
	}

	bl.segs = append(bl.segs, seg)
	return bl.setHotSeg(seg)
}

// splitIfFull splits the BigLog if the currently active segment is full.
func (bl *BigLog) splitIfFull() error {
	if bl.hotSeg.Load().(*segment).IsFull() {
		return bl.split()
	}

	return nil
}

// Sync flushes all data of the currently active segment to disk.
func (bl *BigLog) Sync() error {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	return bl.sync()
}

func (bl *BigLog) sync() error {
	hotSeg := bl.hotSeg.Load().(*segment)
	if flusher, ok := hotSeg.writer.(ioFlusher); ok {
		if err := flusher.Flush(); err != nil {
			return err
		}
	}

	return hotSeg.Sync()
}

// interface to flush bufio.Writer
type ioFlusher interface {
	Flush() error
}

func (bl *BigLog) setHotSeg(seg *segment) (err error) {
	// handle freeing up buffer from previous
	// hot segment and setting up a new one
	hotSeg := bl.hotSeg.Load().(*segment)
	if hotSeg != nil {
		if flusher, ok := hotSeg.writer.(ioFlusher); ok {
			err = flusher.Flush()
			hotSeg.writer = hotSeg.dataFile
		}

		close(hotSeg.notify)
	}

	if bl.bufioSize > 0 {
		seg.writer = bufio.NewWriterSize(seg.dataFile, bl.bufioSize)
	}

	bl.hotSeg.Store(seg)
	return err
}

// Trim removes the oldest segment from the biglog.
func (bl *BigLog) Trim() (err error) {
	bl.mu.Lock()
	defer bl.mu.Unlock()

	if len(bl.segs) < 2 {
		return ErrLastSegment
	}

	if err = bl.segs[0].Delete(false); err != nil {
		return err
	}

	bl.segs = bl.segs[1:]
	return nil
}

// Close frees all resources, rendering the BigLog unusable without
// touching the data persisted on disk.
func (bl *BigLog) Close() error {
	bl.mu.Lock()  // lock writes
	bl.wmu.Lock() // lock new watchers
	bl.rmu.Lock() // lock new readers
	defer bl.mu.Unlock()
	defer bl.wmu.Unlock()
	defer bl.rmu.Unlock()
	return bl.close(false)
}

func (bl *BigLog) close(force bool) (err error) {
	if !force {
		// check if there are readers/watchers using it
		nWatchers := len(bl.watchers.Load().(watcherMap))
		nScanners := len(bl.readers.Load().(readerMap))
		if nWatchers > 0 || nScanners > 0 {
			return ErrBusy
		}

		// Double check on every segment
		for _, s := range bl.segs {
			if s.IsBusy() {
				return ErrBusy
			}
		}
	}

	for _, s := range bl.segs {
		if err = s.Close(); err != nil && !force {
			return err
		}
	}

	bl.segs = nil
	bl.hotSeg.Store((*segment)(nil))

	return nil
}

// Delete closes bl and deletes all segments and all files stored on disk.
func (bl *BigLog) Delete(force bool) (err error) {
	bl.mu.Lock()  // lock writes
	bl.wmu.Lock() // lock new watchers
	bl.rmu.Lock() // lock new readers
	defer bl.mu.Unlock()
	defer bl.wmu.Unlock()
	defer bl.rmu.Unlock()
	return bl.delete(force)
}

func (bl *BigLog) delete(force bool) (err error) {
	err = bl.close(force)
	if err != nil && !force {
		return err
	}

	for _, s := range bl.segs {
		if err = s.Delete(force); err != nil && !force {
			// map busy error
			if err == ErrSegmentBusy {
				err = ErrBusy
			}

			return err
		}
	}

	return os.RemoveAll(bl.dirPath)
}

// locateOffset given an offset returns the segment where the offset resides and
// its relative offset within the segment, or ErrNotFound if it can not be located
// the relative offset is exact, it will not deal with embedded offset conditions.
func (bl *BigLog) locateOffset(offset int64) (seg *segment, RO uint32, err error) {
	i := indexOfSegment(bl.segs, offset)
	if i < 0 {
		return nil, 0, ErrNotFound
	}

	seg = bl.segs[i]

	RO = relative(offset, seg.baseOffset)
	if RO > seg.NRO {
		return nil, 0, ErrNotFound
	}

	return seg, RO, nil
}

func (bl *BigLog) locateTS(TS uint32) (seg *segment, RO uint32, err error) {
	i := indexOfSegmentTS(bl.segs, TS)
	if i < 0 {
		return nil, 0, ErrNotFound
	}

	seg = bl.segs[i]
	l := seg.searchTS(TS)

	return seg, l.RO, nil
}

// notify dispatches change notifications to watchers.
func (bl *BigLog) notify() {
	var hotSeg *segment
	for {
		hotSeg = bl.hotSeg.Load().(*segment)
		if hotSeg == nil {
			return
		}

		// block until we get a notification
		// !ok when the channel closes implies
		// that there is a new hot segment
		_, ok := <-hotSeg.notify
		if !ok {
			continue
		}

		bl.wmu.Lock()
		// non-blocking notify watchers
		for wc := range bl.watchers.Load().(watcherMap) {
			select {
			case wc <- struct{}{}:
			default:
			}
		}
		bl.wmu.Unlock()
	}
}

func (bl *BigLog) addReader(r io.Closer) {
	bl.rmu.Lock()
	defer bl.rmu.Unlock()

	m1 := bl.readers.Load().(readerMap)
	if _, ok := m1[r]; ok {
		return // no-op
	}

	m2 := make(readerMap, len(m1)+1)
	for k, v := range m1 {
		m2[k] = v
	}

	m2[r] = struct{}{}
	bl.readers.Store(m2)
}

func (bl *BigLog) removeReader(r io.Closer) {
	bl.rmu.Lock()
	defer bl.rmu.Unlock()

	m1 := bl.readers.Load().(readerMap)
	if _, ok := m1[r]; !ok {
		return // no-op
	}

	m2 := make(readerMap, len(m1)-1)
	for k, v := range m1 {
		if k != r {
			m2[k] = v
		}
	}

	bl.readers.Store(m2)
}

func (bl *BigLog) addWatcher(wc chan struct{}) {
	bl.wmu.Lock()
	defer bl.wmu.Unlock()

	m1 := bl.watchers.Load().(watcherMap)
	if _, ok := m1[wc]; ok {
		return // no-op
	}

	m2 := make(watcherMap, len(m1)+1)
	for k, v := range m1 {
		m2[k] = v
	}

	m2[wc] = struct{}{}
	bl.watchers.Store(m2)
}

func (bl *BigLog) removeWatcher(wc chan struct{}) {
	bl.wmu.Lock()
	defer bl.wmu.Unlock()

	m1 := bl.watchers.Load().(watcherMap)
	if _, ok := m1[wc]; !ok {
		return // no-op
	}

	m2 := make(watcherMap, len(m1)-1)
	for k, v := range m1 {
		if k != wc {
			m2[k] = v
		}
	}

	bl.watchers.Store(m2)
}

func indexOfSegment(a []*segment, offset int64) int {
	i := sort.Search(len(a), func(i int) bool {
		return a[i].baseOffset > offset
	})

	return i - 1
}

func indexOfSegmentTS(a []*segment, TS uint32) int {
	i := sort.Search(len(a), func(i int) bool {
		return a[i].createdTS > TS
	})

	return i - 1
}

func relative(offset, base int64) uint32 {
	if base > offset {
		panic("base offset out of reach")
	}
	return uint32(offset - base + 1)
}

func absolute(offset uint32, base int64) int64 {
	return int64(offset) + base - 1
}
