// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"hash/crc32"
	"io"
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
func (e *Message) CompVer() uint8 {
	return e.Bytes()[compverPos]
}

// Compression returns the compression encoded in bits 4 to 8 of the header.
func (e *Message) Compression() uint8 {
	return e.CompVer() & 15
}

// Version returns the format version encoded in bits 0 to 3 of the header.
func (e *Message) Version() uint8 {
	return e.CompVer() >> 4
}

// Size returns the total size in bytes of the message.
func (e *Message) Size() int {
	return int(e.PLength() + headerSize)
}

// CRC32 returns the checksum of the payload.
func (e *Message) CRC32() uint32 {
	return enc.Uint32(e.Bytes()[crc32Pos : crc32Pos+4])
}

// PLength returns the length (bytes) of the payload.
func (e *Message) PLength() uint32 {
	return enc.Uint32(e.Bytes()[plenthPos : plenthPos+4])
}

// Payload returns the data bytes.
func (e *Message) Payload() []byte {
	return e.Bytes()[headerSize:]
}

// Bytes returns the entire message casted back to bytes.
func (e *Message) Bytes() []byte {
	return []byte(*e)
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
	r.Read(buf[headerSize:])
	entry = Message(buf)

	return entry, err
}
