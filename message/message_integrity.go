// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package message

import (
	"hash/crc32"
	"strconv"
)

// IntegrityErrorType is the category of possible errors in the data.
type IntegrityErrorType string

const (
	// IntegrityChecksumErr is returned when the checksum in the message
	// header doesn't match the checksum recalculated from the payload.
	IntegrityChecksumErr IntegrityErrorType = "checksum"

	// IntegrityLengthErr is returned when the length in the message
	// header doesn't match the length of the payload.
	IntegrityLengthErr IntegrityErrorType = "length"

	// IntegrityUnknownErr is returned when data can not be read because
	// of an underlying error reading the data.
	IntegrityUnknownErr IntegrityErrorType = "unknown"
)

// IntegrityError is the struct with metadata about an any integrity error found.
type IntegrityError struct {
	Offset   int64              `json:"offset"`
	ODelta   int                `json:"odelta"`
	Type     IntegrityErrorType `json:"type"`
	Expected string             `json:"expected"`
	Actual   string             `json:"actual"`
}

// CheckMessageIntegrity checks the integrity of a single message
func CheckMessageIntegrity(m Message, delta int) *IntegrityError {
	if !m.ChecksumOK() {
		return &IntegrityError{
			ODelta:   delta,
			Type:     IntegrityChecksumErr,
			Expected: strconv.Itoa(int(crc32.ChecksumIEEE(m.Bytes()))),
			Actual:   strconv.Itoa(int(m.CRC32())),
		}
	}

	if int(m.PLength()) != len(m.Payload()) {
		return &IntegrityError{
			ODelta:   delta,
			Type:     IntegrityLengthErr,
			Expected: strconv.Itoa(int(m.PLength())),
			Actual:   strconv.Itoa(len(m.Payload())),
		}
	}

	// TODO check compressed integrity
	return nil
}
