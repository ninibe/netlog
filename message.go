// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"bytes"
	"compress/gzip"
	"hash/crc32"
	"io"
)

const (
	compverPos = 0 // CompVer uint8
	crc32Pos   = 1 // CRC     uint32
	plenthPos  = 5 // PLength uint32
	payloadPos = 9 // Payload []byte
	headerSize = payloadPos
)

type CompressionType uint8

const (
	// CompressionNone is used by single messages
	CompressionNone CompressionType = 0
	// CompressionGzip is used by message sets with gzipped payloads
	CompressionGzip CompressionType = 1
	// CompressionSnappy is used by message sets with snappy payloads
	CompressionSnappy CompressionType = 2
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
	if comp == CompressionNone {
		panic("can not generate message-set without compression")
	}

	// TODO buffer pool?
	buf := &bytes.Buffer{}
	w := gzip.NewWriter(buf)
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

// Unpack takes a batch of messages and returns the split into an slice
// the batch can be either a simple byte sequence or several messages
// compressed into one message.
func Unpack(m Message) ([]Message, error) {

	switch m.Compression() {
	case CompressionNone:
		return unpackSequence(m)

	case CompressionGzip:
		return unpackGzip(m.Payload())

	case CompressionSnappy:
		panic("not yet implemented")
	}

	return nil, nil
}

func unpackSequence(data []byte) (messages []Message, err error) {
	r := bytes.NewReader(data)
	var msg Message

	for {
		msg, err = ReadMessage(r)
		if err != nil {
			break
		}

		messages = append(messages, msg)
	}

	if err == io.EOF {
		return messages, nil
	}

	return messages, err
}

func unpackGzip(data []byte) (messages []Message, err error) {
	r := bytes.NewReader(data)
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	var msg Message

	for {
		msg, err = ReadMessage(gr)
		if err != nil {
			break
		}

		messages = append(messages, msg)
	}

	if err == io.EOF {
		return messages, nil
	}

	return messages, err
}
