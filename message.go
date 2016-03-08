// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"bytes"
	"hash/crc32"
	"io"
	"log"
)

//	CompVer uint8
//	CRC     uint32
//	PLength uint32
//	Payload []byte

const (
	compverPos = 0
	crc32Pos   = 1
	plenthPos  = 5
	payloadPos = 9
)

const (
	headerSize        = 9
	compressionNone   = 0
	compressionGzip   = 1
	compressionSnappy = 2
)

// MessageFromPayload returns a message with the appropriate
// calculated headers from a give data payload.
func MessageFromPayload(p []byte) Message {
	buf := make([]byte, len(p)+headerSize)
	enc.PutUint32(buf[crc32Pos:crc32Pos+4], crc32.ChecksumIEEE(p))
	enc.PutUint32(buf[plenthPos:plenthPos+4], uint32(len(p)))
	copy(buf[payloadPos:], p)
	return Message(buf)
}

// Message the unit of data storage.
type Message []byte

// CompVer returns the first byte which reflects both compression a format version.
func (m *Message) CompVer() uint8 {
	return m.Bytes()[compverPos]
}

// Compression returns the compression encoded in bits 4 to 8 of the header.
func (m *Message) Compression() uint8 {
	return m.CompVer() & 15
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
	header := make([]byte, headerSize)
	n, err := r.Read(header)
	if err != nil {
		return entry, err
	}

	if n != headerSize {
		return entry, io.ErrShortBuffer
	}

	// TODO buffer pool?
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
	case compressionNone:
		return unpackSequence(m)
	case compressionGzip:
		panic("not yet implemented")
	case compressionSnappy:
		panic("not yet implemented")
	}

	return nil, nil
}

func unpackSequence(data []byte) ([]Message, error) {
	r := bytes.NewReader(data)
	var messages []Message

	for {
		msg, err := ReadMessage(r)
		if err != nil {
			break
		}

		if !msg.ChecksumOK() {
			log.Printf("warn: corrupt entry in batch sequence")
			continue
		}

		messages = append(messages, msg)
	}

	return messages, nil
}
