// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/comail/go-uuid/uuid"
	"github.com/ninibe/bigduration"

	"github.com/ninibe/netlog/biglog"
)

const settingsFile = "settings.json"

var enc = binary.BigEndian

//go:generate atomicmapper -pointer -type Topic
//go:generate atomicmapper -pointer -type Scanner
//go:generate atomicmapper -pointer -type netlog/biglog.Streamer

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
}

func newTopic(bl *biglog.BigLog, settings TopicSettings, defaultSettings TopicSettings) *Topic {

	if settings.SegSize == 0 {
		settings.SegSize = defaultSettings.SegSize
	}

	if settings.SegAge.Duration() == 0 {
		settings.SegAge = defaultSettings.SegAge
	}

	t := &Topic{
		settings:  settings,
		name:      bl.Name(),
		bl:        bl,
		writer:    bl,
		scanners:  NewTopicScannerAtomicMap(),
		streamers: NewStreamerAtomicMap(),
	}

	return t
}

// Write implements the io.Writer interface for a Topic.
func (t *Topic) Write(p []byte) (n int, err error) {
	return t.writer.Write(p)
}

// Sync flushes all data to disk.
func (t *Topic) Sync() error {
	return t.bl.Sync()
}

type TopicInfo struct {
	biglog.Info
}

// Info provides all public topic information.
func (t *Topic) Info() (i *biglog.Info, err error) {
	// TODO return own version with readers' information
	return t.bl.Info()
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
	var entry Message
	for {
		entry, err = ReadMessage(r)
		n += int64(entry.Size())
		if err == io.EOF {
			return
		}

		if err != nil {
			log.Printf("error: could not read from stream: %s", err)
			return
		}

		if crc32.ChecksumIEEE(entry.Bytes()) != entry.CRC32() {
			log.Printf("warn: corrupt entry in stream")
			continue
		}

		_, err = t.Write(entry.Bytes())
		if err != nil {
			log.Printf("error: could not write stream: %s", err)
			return
		}
	}
}

// Payload is a utility method to fetch the payload of a single offset.
func (t *Topic) Payload(offset int64) ([]byte, error) {
	reader, _, err := biglog.NewReader(t.bl, offset)
	if err != nil {
		return nil, ErrInvalidOffset
	}

	// TODO unpack embedded offset
	entry, err := ReadMessage(reader)
	if err != nil {
		return nil, err
	}

	// TODO check crc?
	return entry.Payload(), nil
}

// CreateScanner creates a new scanner starting at offset `from`.
func (t *Topic) CreateScanner(from int64) (bs *TopicScanner, err error) {
	bs, err = NewTopicScanner(t.bl, from)
	if bs == nil || err != nil {
		return bs, ExtErr(err)
	}

	bs.ID = uuid.New()
	t.scanners.Set(bs.ID, bs)
	return bs, nil
}

// Scanner returns an existing scanner for the topic given and ID
// or ErrScannerNotFound if it doesn't exists.
func (t *Topic) Scanner(ID string) (bs *TopicScanner, err error) {
	bs, ok := t.scanners.Get(ID)
	if !ok {
		return nil, ErrScannerNotFound
	}
	return bs, nil
}

// DeleteScanner removes the scanner from the topic
func (t *Topic) DeleteScanner(ID string) error {
	_, ok := t.scanners.Get(ID)
	if !ok {
		return ErrScannerNotFound
	}

	t.scanners.Delete(ID)
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

	if str == "beginning" || str == "first" || str == "oldest" || str == "start" {
		return t.bl.Oldest(), nil
	}

	if str == "last" || str == "latest" {
		return t.bl.Latest(), nil
	}

	if str == "end" || str == "now" || str == "" {
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
