// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/comail/go-uuid/uuid"
	"github.com/ninibe/bigduration"
	"golang.org/x/net/context"

	"github.com/ninibe/netlog/biglog"
	"github.com/ninibe/netlog/message"
)

const settingsFile = "settings.json"

var enc = binary.BigEndian

//go:generate atomicmapper -pointer -type Topic
//go:generate atomicmapper -type TopicScanner

// Topic is a log of linear messages.
type Topic struct {
	name      string
	settings  TopicSettings
	bl        *biglog.BigLog
	writer    io.Writer
	scanners  *TopicScannerAtomicMap
	streamers *StreamerAtomicMap
}

// TopicSettings holds the tunable settings of a topic.
type TopicSettings struct {
	// SegAge is the age at after which old segments are discarded.
	SegAge bigduration.BigDuration `json:"segment_age,ommitempty"`
	// SegSize is the size at which a new segment should be created.
	SegSize int64 `json:"segment_size,ommitempty"`
	// BatchNumMessages is the maximum number of messages to be batched.
	BatchNumMessages int `json:"batch_num_messages,ommitempty"`
	// BatchInterval is the interval at which batched messages are flushed to disk.
	BatchInterval bigduration.BigDuration `json:"batch_interval,ommitempty"`
	// CompressionType allows to specify how batches are compressed.
	CompressionType message.CompressionType `json:"compression_type,ommitempty"`
}

func newTopic(bl *biglog.BigLog, settings TopicSettings, defaultSettings TopicSettings) *Topic {

	if settings.SegSize == 0 {
		settings.SegSize = defaultSettings.SegSize
	}

	if settings.SegAge.Duration() == 0 {
		settings.SegAge = defaultSettings.SegAge
	}

	if settings.BatchNumMessages == 0 {
		settings.BatchNumMessages = defaultSettings.BatchNumMessages
	}

	if settings.BatchInterval.Duration() == 0 {
		settings.BatchInterval = defaultSettings.BatchInterval
	}

	if settings.CompressionType == message.CompressionDefault {
		settings.CompressionType = defaultSettings.CompressionType
	}

	t := &Topic{
		settings:  settings,
		name:      bl.Name(),
		bl:        bl,
		writer:    bl,
		scanners:  NewTopicScannerAtomicMap(),
		streamers: NewStreamerAtomicMap(),
	}

	if settings.BatchNumMessages > 1 ||
		settings.BatchInterval.Duration() > 0 {
		t.writer = newMessageBuffer(bl, settings)
	}

	t.restorePersistedReaders()
	return t
}

// Write implements the io.Writer interface for a Topic.
func (t *Topic) Write(p []byte) (n int, err error) {
	return t.writer.Write(p)
}

// WriteN writes a set of N messages to the Topic
func (t *Topic) WriteN(p []byte, n int) (written int, err error) {
	return t.bl.WriteN(p, n)
}

// Sync flushes all data to disk.
func (t *Topic) Sync() error {
	err := t.FlushBuffered()
	if err != nil {
		return err
	}

	return t.bl.Sync()
}

// Name returns the Topic's name, which maps to the folder name
func (t *Topic) Name() string {
	return t.name
}

// TopicInfo returns the topic information including information
// about size, segments, scanners and streamers
type TopicInfo struct {
	*biglog.Info
	Scanners map[string]TScannerInfo `json:"scanners"`
}

// Info provides all public topic information.
func (t *Topic) Info() (i *TopicInfo, err error) {
	bi, err := t.bl.Info()
	if err != nil {
		return nil, err
	}

	scanners := t.scanners.GetAll()
	scanInfo := make(map[string]TScannerInfo, len(scanners))
	for k, v := range scanners {
		scanInfo[k] = v.Info()
	}

	inf := &TopicInfo{
		Info:     bi,
		Scanners: scanInfo,
	}

	return inf, nil
}

// interface to flush bufio.Writer
type ioFlusher interface {
	Flush() error
}

// FlushBuffered flushes all buffered messages into the BigLog.
// Notice that the BigLog might have a buffer on it's own that this
// function does not flush, so calling this does not mean the data
// has been stored on disk.
func (t *Topic) FlushBuffered() error {
	if flusher, ok := t.writer.(ioFlusher); ok {
		return flusher.Flush()
	}

	return nil
}

// CheckSegments is called by the runner and discards
// or splits segments when conditions are met.
func (t *Topic) CheckSegments() error {
	blInfo, err := t.bl.Info()
	if err != nil {
		return err
	}

	err = t.checkSegmentsAge(blInfo)
	if err != nil {
		return err
	}

	return t.checkSegmentsSize(blInfo)
}

// Check that the oldest segment is not too old.
func (t *Topic) checkSegmentsAge(bi *biglog.Info) error {
	if t.settings.SegAge.Duration() <= 0 {
		return nil
	}

	if len(bi.Segments) < 2 {
		return nil
	}

	if t.settings.SegAge.From(bi.Segments[0].ModTime).After(time.Now()) {
		return nil
	}

	log.Printf("info: removing old segment on %q", t.name)
	return t.bl.Trim()
}

// Check that the hot segment is not too big.
func (t *Topic) checkSegmentsSize(bi *biglog.Info) error {
	if t.settings.SegSize <= 0 {
		return nil
	}

	if bi.Segments[len(bi.Segments)-1].DataSize <= t.settings.SegSize {
		return nil
	}

	log.Printf("info: creating new segment on %q", t.name)
	return t.bl.Split()
}

// ReadFrom reads an entry or stream of entries from r until EOF is reached
// writes the entry/stream into the topic is the entry is valid.
// The return value n is the number of bytes read.
// It implements the io.ReaderFrom interface.
func (t *Topic) ReadFrom(r io.Reader) (n int64, err error) {
	var m message.Message
	for {
		m, err = message.ReadMessage(r)
		n += int64(m.Size())
		if err == io.EOF {
			return
		}

		if err != nil {
			log.Printf("error: could not read from stream: %s", err)
			return
		}

		if !m.ChecksumOK() {
			log.Print("warn: corrupt entry in stream")
			continue
		}

		_, err = t.Write(m.Bytes())
		if err != nil {
			log.Printf("error: could not write stream: %s", err)
			return
		}
	}
}

// Payload is a utility method to fetch the payload of a single offset.
func (t *Topic) Payload(offset int64) ([]byte, error) {
	r, ret, err := biglog.NewReader(t.bl, offset)
	if err != nil && err != biglog.ErrEmbeddedOffset {
		return nil, err
	}

	entry, err := message.ReadMessage(r)
	if err != nil {
		return nil, err
	}

	// extract list of messages out of the stored entry
	msgs, err := message.Unpack(entry)
	if err != nil {
		return nil, err
	}

	// ret is the first offset of the returned list
	// offset-ret = position of message within the list
	m := msgs[offset-ret]
	if !m.ChecksumOK() {
		return nil, ErrCRC
	}

	return m.Payload(), nil
}

// NewScanner creates a new scanner starting at offset `from`. If `persist` is true,
// the scanner and it's state will survive server restarts
func (t *Topic) NewScanner(from int64, persist bool) (ts TopicScanner, err error) {
	return t.createScanner(uuid.New(), from, persist)
}

func (t *Topic) createScanner(ID string, from int64, persist bool) (ts TopicScanner, err error) {
	defer func() {
		if err != nil {
			log.Printf("warn: failed to create scanner %s:%d err: %s", t.Name(), from, err)
		}
	}()

	if from < 0 {
		log.Printf("warn: can't create scanner from negative offset %d", from)
		return nil, ErrInvalidOffset
	}

	log.Printf("info: creating scanner from offset %d", from)
	ts, err = NewTopicScanner(t, ID, from, persist)
	if ts == nil || err != nil {
		return ts, ExtErr(err)
	}

	// register scanner in this topic
	t.scanners.Set(ts.Info().ID, ts)

	log.Printf("info: created scanner from %s:%d", t.Name(), from)
	return ts, nil
}

// Scanner returns an existing scanner for the topic given and ID
// or ErrScannerNotFound if it doesn't exists.
func (t *Topic) Scanner(ID string) (ts TopicScanner, err error) {
	ts, ok := t.scanners.Get(ID)
	if !ok {
		return nil, ErrScannerNotFound
	}
	return ts, nil
}

// DeleteScanner removes the scanner from the topic
func (t *Topic) DeleteScanner(ID string) (err error) {
	defer func() {
		if err != nil {
			log.Printf("warn: failed to delete scanner %s from %s err: %s", ID, t.Name(), err)
		}
	}()

	log.Printf("info: deleting scanner %s from %q", ID, t.Name())
	sc, ok := t.scanners.Get(ID)
	if !ok {
		return ErrScannerNotFound
	}

	err = sc.Close()
	if err != nil {
		return err
	}

	t.scanners.Delete(ID)

	log.Printf("info: deleted scanner %s from %q", ID, t.Name())
	return nil
}

// ParseOffset converts an offset string into a numeric precise offset
// 'beginning', 'first' or 'oldest' return the lowest available offset in the topic
// 'last' or 'latest' return the highest available offset in the topic
// 'end' or 'now' return the next offset to be written in the topic
// numeric string values are directly converted to integer
// duration notation like "1day" returns the first offset available since 1 day ago.
func (t *Topic) ParseOffset(str string) (int64, error) {
	str = strings.ToLower(str)

	if str == "" ||
		str == "beginning" ||
		str == "first" ||
		str == "oldest" ||
		str == "start" {
		return t.bl.Oldest(), nil
	}

	if str == "last" || str == "latest" {
		return t.bl.Latest(), nil
	}

	if str == "end" || str == "now" {
		return t.bl.Latest() + 1, nil
	}

	// numeric value?
	offset, err := strconv.ParseInt(str, 10, 0)
	if err == nil {
		return offset, nil
	}

	// time value?
	bd, err := bigduration.ParseBigDuration(str)
	if err != nil {
		return -1, ErrInvalidOffset
	}

	offset, err = t.bl.After(bd.Until(time.Now()))
	if err != nil {
		return -1, ErrInvalidOffset
	}

	return offset, nil
}

// CheckIntegrity scans the topic and checks for inconsistencies in the data
func (t *Topic) CheckIntegrity(ctx context.Context, from int64) ([]*message.IntegrityError, error) {
	log.Printf("info: checking integrity of topic %q", t.Name())

	ic, err := NewIntegrityChecker(t, from)
	if err != nil {
		return nil, ExtErr(err)
	}

	defer logClose(ic)
	iErrs := ic.Check(ctx)

	log.Printf("info: integrity check finished for topic %q. Found %d errors.", t.Name(), len(iErrs))
	return iErrs, nil
}

// DirPath returns the absolute path to
// the folder with the topic's files
func (t *Topic) DirPath() string {
	return t.bl.DirPath()
}

func (t *Topic) scannerPath(ID string) string {
	return filepath.Join(t.readersDir(), fmt.Sprintf(scannerPattern, ID))
}

func (t *Topic) readersDir() string {
	return filepath.Join(t.DirPath(), "readers")
}

func (t *Topic) restorePersistedReaders() {
	dirfs, err := ioutil.ReadDir(t.readersDir())
	if err != nil {
		return
	}

	var ID string
	for _, f := range dirfs {
		switch filepath.Ext(f.Name()) {
		case ".scanner":
			_, err = fmt.Sscanf(f.Name(), scannerPattern, &ID)
			if err != nil {
				log.Printf("error: unable to parse scanner: %s", err)
				continue
			}

			last := offsetFromFile(t.scannerPath(ID))
			from := last + 1
			if last < t.bl.Oldest() {
				from = t.bl.Oldest()
			}
			_, err := t.createScanner(ID, from, true)
			if err != nil {
				log.Printf("error: unable to restore scanner %s: %s", ID, err)
				continue
			}

			log.Printf("info: restored scanner %s on %s:%d", ID, t.name, from)
		default:
			log.Printf("error: unknown file: %s", f.Name())
		}
	}
}

func offsetFromFile(filePath string) int64 {
	f, err := os.Open(filePath)
	if err != nil {
		return -1
	}

	defer logClose(f)

	b, _ := ioutil.ReadAll(f)
	if len(b) == 8 {
		return int64(enc.Uint64(b))
	}

	return -1
}
