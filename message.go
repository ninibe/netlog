// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"bytes"
	"compress/gzip"
	"hash/crc32"
	"io"

	"github.com/golang/snappy"
)

const (
	compverPos = 0 // CompVer uint8
	crc32Pos   = 1 // CRC     uint32
	plenthPos  = 5 // PLength uint32
	payloadPos = 9 // Payload []byte
	headerSize = payloadPos
)

// CompressionType indicates a type of compression for message sets
type CompressionType uint8

const (
	// CompressionDefault is used when falling back to the default compression of the system.
	CompressionDefault CompressionType = 0
	// CompressionNone is used by messages sets with uncompressed payloads
	CompressionNone CompressionType = 1
	// CompressionGzip is used by message sets with gzipped payloads
	CompressionGzip CompressionType = 2
	// CompressionSnappy is used by message sets with snappy payloads
	CompressionSnappy CompressionType = 3
)

// MessageFromPayload returns a message with the appropriate calculated headers from a give data payload.
func MessageFromPayload(p []byte) Message {
	buf := make([]byte, len(p)+headerSize)
	enc.PutUint32(buf[crc32Pos:crc32Pos+4], crc32.ChecksumIEEE(p))
	enc.PutUint32(buf[plenthPos:plenthPos+4], uint32(len(p)))
	copy(buf[payloadPos:], p)
	return Message(buf)
}

// MessageSet returns a new message with a batch of compressed messages as payload
// Compression will compress the payload and set the compression header, please be ware that compression
// at this level is only meant for batching several messages into a single message-set in increase throughput.
// MessageSet will panic if a compression type is not provided, since nothing would indicate to streaming
// clients that further messages are embedded in the payload.
func MessageSet(msgs []Message, comp CompressionType) Message {
	// TODO buffer pool?
	buf := &bytes.Buffer{}
	var w io.WriteCloser

	switch comp {
	case CompressionNone:
		w = NopWCloser(buf)

	case CompressionGzip:
		w = gzip.NewWriter(buf)

	case CompressionSnappy:
		w = snappy.NewWriter(buf)

	default:
		panic("invalid compression type")
	}

	for _, m := range msgs {
		w.Write(m)
	}

	w.Close()

	m := MessageFromPayload(buf.Bytes())
	m[compverPos] = byte(comp)

	return m
}

// Message the unit of data storage.
type Message []byte

// CompVer returns the first byte which reflects both compression a format version.
func (m *Message) CompVer() uint8 {
	return m.Bytes()[compverPos]
}

// Compression returns the compression encoded in bits 4 to 8 of the header.
func (m *Message) Compression() CompressionType {
	return CompressionType(m.CompVer() & 15)
}

// Version returns the format version encoded in bits 0 to 3 of the header.
func (m *Message) Version() uint8 {
	return m.CompVer() >> 4
}

// Size returns the total size in bytes of the message.
func (m *Message) Size() int {
	return int(m.PLength() + headerSize)
}

// CRC32 returns the checksum of the payload.
func (m *Message) CRC32() uint32 {
	return enc.Uint32(m.Bytes()[crc32Pos : crc32Pos+4])
}

// PLength returns the length (bytes) of the payload.
func (m *Message) PLength() uint32 {
	return enc.Uint32(m.Bytes()[plenthPos : plenthPos+4])
}

// Payload returns the data bytes.
func (m *Message) Payload() []byte {
	return m.Bytes()[headerSize:]
}

// Bytes returns the entire message casted back to bytes.
func (m *Message) Bytes() []byte {
	return []byte(*m)
}

// ChecksumOK recalculates the CRC from the payload
// and compares it with the one stored in the header
func (m *Message) ChecksumOK() bool {
	return crc32.ChecksumIEEE(m.Payload()) == m.CRC32()
}

// ReadMessage reads a message from r and returns it
// if the message is compressed it does not attempt to unpack the contents.
func ReadMessage(r io.Reader) (entry Message, err error) {

	// TODO buffer pool?
	header := make([]byte, headerSize)
	n, err := r.Read(header)
	if err != nil {
		return entry, err
	}

	if n != headerSize {
		return entry, io.ErrShortBuffer
	}

	entry = Message(header)
	buf := make([]byte, entry.Size())
	copy(buf, header)
	_, _ = r.Read(buf[headerSize:])
	entry = Message(buf)

	return entry, err
}

// Unpack takes a message-set and returns a slice with the component messages.
func Unpack(set Message) ([]Message, error) {
	if set.Compression() > 0 {
		// unpack compressed payload
		return unpack(set.Payload(), set.Compression())
	}

	// if instead of a compressed message it's a sequence
	// of uncompressed ones, reading the compression flag
	// is effectively reading the flag on the first message.
	// For a sequence we can unpack the data as-is.
	return unpack(set, CompressionNone)
}

func unpack(data []byte, comp CompressionType) (msgs []Message, err error) {
	var r io.Reader = bytes.NewReader(data)

	switch comp {
	case 0: // not a set
	case CompressionNone:
	case CompressionGzip:
		r, err = gzip.NewReader(r)
		if err != nil {
			return nil, err
		}

	case CompressionSnappy:
		r = snappy.NewReader(r)

	default:
		return nil, ErrInvalidCompression
	}

	// close reader if possible on exit
	defer func() {
		if r, ok := r.(io.Closer); ok {
			r.Close()
		}
	}()

	var m Message
	for {
		m, err = ReadMessage(r)
		if err != nil {
			break
		}

		msgs = append(msgs, m)
	}

	if err == io.EOF {
		return msgs, nil
	}

	return msgs, err
}

// NopWCloser returns a WriteCloser with a no-op
// Close method wrapping the provided Writer w.
func NopWCloser(w io.Writer) io.WriteCloser {
	return nopWCloser{w}
}

type nopWCloser struct {
	io.Writer
}

func (nopWCloser) Close() error { return nil }
