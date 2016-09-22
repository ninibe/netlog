// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"

	"launchpad.net/gommap"
)

// Logger is the logger instance used by BigLog in case of error.
var Logger = log.New(os.Stderr, "BIGLOG ", log.LstdFlags)

// Glossary
// Entry: One write, either in the index or data files
// RO: relative offset (entry offset within a segment)
// fRO: relative offset found (parent offset of an embedded offset)
// FO: file offset (byte offset within os.File)
// iRO: relative offset read from index file
// NRO: next relative offset (highest RO in segment)
// iFO: offset in bytes inside index file
// dFO: offset in bytes inside data file
// TS: offset timestamp

var (
	// ErrSegmentFull is returned when the index does not have capacity left.
	ErrSegmentFull = errors.New("biglog: segment full")

	// ErrSegmentBusy is returned when trying to delete a segment that is being read.
	ErrSegmentBusy = errors.New("biglog: segment busy")

	// ErrLoadSegment is returned when segment files could not be loaded, the reason should be logged.
	ErrLoadSegment = errors.New("biglog: failed to load segment")

	// ErrRONotFound is returned when the requested relative offset is not in the segment.
	ErrRONotFound = errors.New("biglog: relative offset not found in segment")

	// ErrROInvalid is returned when the requested offset is out of range.
	ErrROInvalid = errors.New("biglog: invalid relative offset 0 < RO < 4294967295")
)

var (
	indexPattern  = "%020d.index"
	dataPattern   = "%020d.data"
	mmapProtFlags = gommap.PROT_READ | gommap.PROT_WRITE
	mmapMapFlags  = gommap.MAP_SHARED
)

const (
	ow = 4      // offset width (length in bytes of a relative offset in the index)
	tw = ow + 4 // time-offset width (length in bytes of a timestamp in the index plus the offset)
	iw = tw + 8 // index width (length in bytes of an index entry)
)

var enc = binary.BigEndian

// A segment is the main abstraction over a block of data.
// Each segment contains both a data and an index file.
// The index file is memory mapped for fast access.
type segment struct {
	readers   *int32
	indexPath string
	dataPath  string
	index     gommap.MMap // memory mapped index(RelOffset 4 bytes -> FileOffset 8 bytes)

	dataFile  *os.File
	indexFile *os.File
	writer    io.Writer

	indexSize uint32
	createdTS uint32 // timestamp of the first entry in the log

	baseOffset int64  // global offset for the first entry in this segment
	NdFO       int64  // next data file offset (dFO of NRO)
	NRO        uint32 // next relative offset written
	NiFO       uint32 // next offset in the index file (iFO of NRO)

	notify chan struct{} // channel to notify write events
}

// createSegment creates and loads a new segment at the given dirPath.
// baseOffset is used to determine the segments file names.
// maxIndexEntries is the maximum number of entries which is used to allocate the
// entire index file. The segment is immediately loaded and ready to be used if
// no error is returned.
func createSegment(dirPath string, maxIndexEntries int, baseOffset int64) (*segment, error) {
	var (
		idxName  = fmt.Sprintf(indexPattern, baseOffset)
		dataName = fmt.Sprintf(dataPattern, baseOffset)
		idxPath  = filepath.Join(dirPath, idxName)
		dataPath = filepath.Join(dirPath, dataName)
	)

	err := createSegIndex(idxPath, maxIndexEntries)
	if err != nil {
		return nil, err
	}

	err = createSegData(dataPath)
	if err != nil {
		return nil, err
	}

	return loadSegment(idxPath)
}

// loadSegment loads a segment given the path to its index file.
// The path to the data file is calculated based on the given indexPath.
// The returned segment is fully initialized and ready to be used
// immediately if no error is returned.
//
// ErrLoadSegment is returned if the indexPath does not conform to the
// indexPattern or the corresponding index or data file can not be opened
// or memory mapped.
func loadSegment(indexPath string) (*segment, error) {
	dirPath, indexName := filepath.Split(indexPath)

	var baseOffset int64
	_, err := fmt.Sscanf(indexName, indexPattern, &baseOffset)
	if err != nil {
		Logger.Printf("error: invalid index name '%s' %s", indexPath, err)
		return nil, ErrLoadSegment
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR, 0666)
	if err != nil {
		Logger.Printf("error: '%s' %s", indexPath, err)
		return nil, ErrLoadSegment
	}

	dataName := fmt.Sprintf(dataPattern, baseOffset)
	dataPath := filepath.Join(dirPath, dataName)
	dataFile, err := os.OpenFile(dataPath, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		Logger.Printf("error: '%s' %s", indexPath, err)
		return nil, ErrLoadSegment
	}

	var readers int32
	seg := &segment{
		readers:    &readers,
		baseOffset: baseOffset,
		indexFile:  indexFile,
		indexPath:  indexPath,
		dataFile:   dataFile,
		dataPath:   dataPath,
		writer:     dataFile,
		notify:     make(chan struct{}, 1),
	}

	seg.index, err = gommap.Map(seg.indexFile.Fd(), mmapProtFlags, mmapMapFlags)
	if err != nil {
		Logger.Printf("error: can't MMAP index: %s", err)
		_ = seg.indexFile.Close()
		_ = seg.dataFile.Close()
		return nil, ErrLoadSegment
	}

	seg.indexSize = uint32(len(seg.index))

	// TODO find next offsets only for hot segment to speed up boot
	seg.setNextOffsets()
	seg.setCreatedTS()

	return seg, nil
}

// ReadAt reads len(b) bytes from the data file starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (s *segment) ReadAt(b []byte, off int64) (n int, err error) {
	return s.dataFile.ReadAt(b, off)
}

// setNextOffsets reads the last offsets (entry and byte) used in the index,
// useful when loading an existing segment from disk.
//
// Segment index:
//            00 01 | 0f ff | 00 10
//            00 02 | 0f ff | 00 1b
//   NRO=5 -> 00 05 | 00 00 | 00 c2  <- NiFO=12 NdFO=c2
//            00 00 | 00 00 | 00 00  <- Unused
//
func (s *segment) setNextOffsets() {
	i := s.indexOfNRO()
	s.NRO, _, s.NdFO = readEntry(s.index[i:])
	s.NiFO = uint32(i)
}

// setCreatedTS assigns the entire segments creation timestamp to be the
// timestamp of the first index entry.
func (s *segment) setCreatedTS() {
	_, s.createdTS, _ = readEntry(s.index)
}

// WriteN writes a batch of n entries from b to the segment.
// It returns the number of bytes written from b and any error encountered.
func (s *segment) WriteN(b []byte, n uint32) (written int, err error) {
	if written, err = s.write(b); err != nil {
		return 0, err
	}

	s.updateIndex(n, int64(written))
	return written, err
}

// write appends len(b) bytes to the segment.
// ErrSegmentFull is returned if the segment is full.
// Note that the index must be updated separately (using updateIndex)
func (s *segment) write(b []byte) (int, error) {
	if s.NiFO >= s.indexSize {
		return 0, ErrSegmentFull
	}

	return s.writer.Write(b)
}

// ReadFrom reads data from src until EOF or an error is encountered.
// All read data is indexed as a singly entry.
func (s *segment) ReadFrom(src io.Reader) (n int64, err error) {
	n, err = io.Copy(s.dataFile, src)
	if n > 0 {
		s.updateIndex(1, n)
	}

	return n, err
}

// updateIndex appends to the index file the new relative offset
// `entries` represents the numbers of entries written. how much RO advances
// `length` represents the total number of bytes written. how much dFO advances
// A new index entry is created and NRO/watermark advanced
func (s *segment) updateIndex(entries uint32, length int64) {
	if s.NRO == 0 {
		panic("0 NRO")
	}

	// write timestamp of the current write
	writeEntryTS(s.index[s.NiFO:], uint32(time.Now().Unix()))

	// advance index offsets
	s.NRO += entries
	s.NdFO += length

	// atomic access required for the index head as it's used by IndexReader
	atomic.AddUint32(&s.NiFO, iw)

	// Write next relative offset
	writeEntry(s.index[s.NiFO:], s.NRO, s.NdFO)

	// non-blocking change notification
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// writeEntry writes the relative offset and data file offset into the entry.
//
// Memory layout of the entry:
//                   ow                tw                iw
//         iRO       |      iTS        |       dFO       |
//   [ 00 00 00 07 ]   [ 00 00 02 b1 ]   [ 00 00 02 b1 ]
//   [   0 : ow    ]   [   ow : tw   ]   [   tw : iw   ] <- mmap slice address
//
// Legend:
//   iRO = relative offset
//   iTS = unix timestamp
//   dFO = data file offset
//   ow  = offset width
//   tw  = time-offset width
//   iw  = complete index width
func writeEntry(entry []byte, relativeOffset uint32, dataFileOffset int64) {
	enc.PutUint32(entry[0:ow], relativeOffset)
	enc.PutUint64(entry[tw:iw], uint64(dataFileOffset))
}

// writeEntryTS writes the timestamp into an entry using the same memory layout
// as writeEntry.
func writeEntryTS(entry []byte, timestamp uint32) {
	enc.PutUint32(entry[ow:tw], timestamp)
}

// readEntry reads all information from a single entry using the memory layout
// documented in the writeEntry function.
func readEntry(entry []byte) (relativeOffset, timestamp uint32, dataFileOffset int64) {
	relativeOffset = enc.Uint32(entry[0:ow])
	timestamp = enc.Uint32(entry[ow:tw])
	dataFileOffset = int64(enc.Uint64(entry[tw:iw]))
	return
}

// Sync flushes changes made to the data and index file to disk.
// Without calling this method, there are no guarantees that changes made to
// the segment are definitively persisted. Flushing is done synchronously
// and will be finished once the function returns.
func (s *segment) Sync() error {
	if err := s.dataFile.Sync(); err != nil {
		return err
	}

	return s.index.Sync(gommap.MS_SYNC)
}

// IsFull returns true when the index does not accept more writes.
func (s *segment) IsFull() bool {
	return s.NiFO+iw >= s.indexSize
}

// IsBusy returns true when the segments has at least one active reader.
func (s *segment) IsBusy() bool {
	return atomic.LoadInt32(s.readers) > 0
}

// Close closes the underlying index and data files.
// If the segment still has active readers Close returns ErrSegmentBusy.
// A segment can only be closed once and does not accept any reads or
// writes after it has been closed successfully.
func (s *segment) Close() error {
	if s.IsBusy() {
		return ErrSegmentBusy
	}

	dErr := s.dataFile.Close()
	iErr := s.indexFile.Close()

	if dErr != nil {
		return dErr
	}

	return iErr
}

// Delete closes the segment and removes all underlying resources.
// Set force to true to ignore any closing errors and delete all data anyway.
func (s *segment) Delete(force bool) error {
	if err := s.Close(); err != nil && !force {
		return err
	}

	if err := os.Remove(s.indexPath); err != nil {
		return err
	}

	return os.Remove(s.dataPath)
}

type lookupRes struct {
	RO  uint32
	TS  uint32
	fRO uint32
	iFO uint32
	dFO int64
}

func (s *segment) Lookup(RO uint32) (l *lookupRes, err error) {

	// invalid offset
	if RO == 0 {
		return nil, ErrROInvalid
	}

	// lookup too far ahead
	if RO > s.NRO {
		return nil, ErrRONotFound
	}

	// before diving into binary search, first try to find the
	// highest possible entry in the index for this relative offset.
	// it should be an exact match if there was no batching.
	maxIFO := (RO - 1) * iw
	maxRO, TS, dFO := readEntry(s.index[maxIFO:])

	// found it!
	if maxRO == RO {
		l := &lookupRes{
			RO:  RO,
			TS:  TS,
			fRO: RO,
			iFO: maxIFO,
			dFO: dFO,
		}
		return l, nil
	}

	// malformed index
	// if e.g: index max entry 7 has max relative offset 5 (0 means it is not set),
	// means that there are more entries written than offsets,
	// which should not be possible.
	if maxRO != 0 && maxRO < (RO-1) {
		Logger.Printf("error: maxRO[%d] < (RO[%d] - 1)", maxRO, RO)
		panic("relative offset found too far ahead in the index")
	}

	return s.searchRO(RO)
}

// If the indexed relative offset [iRO] jumps to a lower
// value, return iRO instead and an error indicating the offset is embedded.
func (s *segment) searchRO(RO uint32) (l *lookupRes, err error) {
	i := s.indexOfRO(RO)
	iRO, TS, dFO := readEntry(s.index[i:])

	if iRO < RO {
		err = ErrEmbeddedOffset
	}

	l = &lookupRes{
		RO:  RO,
		TS:  TS,
		fRO: iRO,
		iFO: uint32(i),
		dFO: dFO,
	}

	return l, err
}

// searchTS will return the first offset after a given timestamp
// since the timestamp is read from the index the offset can never
// be embedded.
func (s *segment) searchTS(TS uint32) (l *lookupRes) {
	i := s.indexOfTS(TS)
	iRO, iTS, dFO := readEntry(s.index[i:])

	return &lookupRes{
		RO:  iRO,
		TS:  iTS,
		fRO: iRO,
		iFO: uint32(i),
		dFO: dFO,
	}
}

// indexOfNRO returns the relative offset within the index file at which the
// next entry can be written.
// Internally this uses binary search over the memory mapped index file to
// find the first index entry that has a relative offset of zero.
func (s *segment) indexOfNRO() int {
	i := sort.Search(int(s.indexSize)/iw, func(i int) bool {
		iRO, _, _ := readEntry(s.index[i*iw:])
		return iRO == 0
	})

	return (i - 1) * iw
}

// indexOfRO returns the file offset in the index of the entry that contains the given RO
// if such an entry does not exist, return the previous (lower) existing RO
func (s *segment) indexOfRO(RO uint32) int {
	i := sort.Search(int(s.indexSize)/iw, func(i int) bool {
		iRO, _, _ := readEntry(s.index[i*iw:])
		return iRO > RO || iRO == 0
	})

	return (i - 1) * iw
}

// indexOfTS returns the file offset in the index of the entry that contains the given TS
// if such an entry does not exist, return the next (higher) TS
func (s *segment) indexOfTS(TS uint32) int {
	i := sort.Search(int(s.indexSize)/iw, func(i int) bool {
		_, iTS, _ := readEntry(s.index[i*iw:])
		return iTS > TS || iTS == TS || iTS == 0
	})

	return i * iw
}

// indexOfDFO returns the file offset in the index of the entry that contains the given dFO
// if such entry an does not exist, return the previous (lower) existing dFO
func (s *segment) indexOfDFO(dFO int64) int {
	// special case
	if dFO == 0 {
		return 0
	}

	i := sort.Search(int(s.indexSize)/iw, func(i int) bool {
		_, _, iDFO := readEntry(s.index[i*iw:])
		return iDFO > dFO || iDFO == 0
	})

	// special case when index between 0 and 1 then i = 0
	if i == 0 {
		return 0
	}

	return (i - 1) * iw
}

// if the process unexpectedly terminates while writing data before the index can be updated
// this will lead to a corrupt entry on the next write. this function checks that NdFO indeed
// corresponds with the end of the data file as it should. It corrects the data file otherwise.
func (s *segment) healthCheckPartialWrite() error {

	in, err := s.Info()
	if err != nil {
		return err
	}

	// all good
	if s.NdFO == in.DataSize {
		return nil
	}

	Logger.Printf("warn: data file %d bytes larger than index. rebuilding...", in.DataSize-s.NdFO)

	/// prepare new data file
	tmpDataPath := s.dataPath + ".temp"
	err = createSegData(tmpDataPath)
	if err != nil {
		return err
	}

	tmpDataFile, err := os.OpenFile(tmpDataPath, os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return err
	}

	// copy only known data
	_, _ = s.dataFile.Seek(0, 0)
	written, err := io.Copy(tmpDataFile, io.LimitReader(s.dataFile, s.NdFO))
	if err != nil {
		log.Printf("alert: Needed to write %d bytes. Wrote %d bytes Err: %s", s.NdFO, written, err)
		return err
	}

	if err = tmpDataFile.Close(); err != nil {
		return err
	}

	// remove old file
	if err = os.Remove(s.dataPath); err != nil {
		return err
	}

	if err = os.Rename(tmpDataPath, s.dataPath); err != nil {
		return err
	}

	s.dataFile, err = os.OpenFile(s.dataPath, os.O_RDWR|os.O_APPEND, 0666)
	return err
}

// createSegIndex creates a new index file at path initializing it
// such that it fits maxIndexEntries.
func createSegIndex(path string, maxIndexEntries int) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}

	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	init := make([]byte, maxIndexEntries*iw)
	writeEntry(init, 1, 0)
	_, err = f.Write(init)

	return err
}

// createSegData creates a new empty data file at the path.
func createSegData(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0666)
	if err != nil {
		return err
	}

	return f.Close()
}

// SegInfo contains information about a segment as returned by segment.Info().
type SegInfo struct {
	FirstOffset int64     `json:"first_offset"`
	DiskSize    int64     `json:"disk_size"`
	DataSize    int64     `json:"data_size"`
	ModTime     time.Time `json:"mod_time"`
}

// Info returns a SegInfo struct with all information about the segment.
func (s *segment) Info() (*SegInfo, error) {

	ifi, err := s.indexFile.Stat()
	if err != nil {
		return nil, err
	}

	dfi, err := s.dataFile.Stat()
	if err != nil {
		return nil, err
	}

	si := &SegInfo{
		FirstOffset: s.baseOffset,
		DiskSize:    ifi.Size() + dfi.Size(),
		DataSize:    dfi.Size(),
		ModTime:     dfi.ModTime(),
	}

	return si, nil
}
