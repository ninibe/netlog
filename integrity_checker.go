// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"hash/crc32"
	"strconv"

	"github.com/ninibe/netlog/biglog"
	"golang.org/x/net/context"
)

// IntegrityErrorType is the category of possible errors in the data.
type IntegrityErrorType string

const (
	errLimit = 1000

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

// IntegrityChecker is used to check the integrity of an entire topic.
type IntegrityChecker struct {
	sc *biglog.Scanner
}

// NewIntegrityChecker creates a new integrity checker for a given topic.
func NewIntegrityChecker(t *Topic, from int64) (*IntegrityChecker, error) {
	sc, err := biglog.NewScanner(t.bl, from)
	if err != nil {
		return nil, err
	}

	return &IntegrityChecker{sc: sc}, nil
}

// Check reads all data collecting errors which then returns.
// Is recommended to pass a cancellable context since this operation can be slow.
func (ic *IntegrityChecker) Check(ctx context.Context) (errors []*IntegrityError) {
	for {
		if len(errors) >= errLimit {
			return errors
		}

		select {
		case <-ctx.Done():
			break
		default:
		}

		m, o, d, err := ic.scan()
		if err == ErrEndOfTopic {
			break
		}

		if err != nil {
			errors = append(errors, &IntegrityError{
				Offset: o,
				ODelta: d,
				Type:   IntegrityUnknownErr,
				Actual: err.Error(),
			})

			continue
		}

		iErr := CheckMessageIntegrity(m, d)
		if iErr != nil {
			iErr.Offset = o
			errors = append(errors, iErr)
		}
	}

	return errors
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
			Actual:   strconv.Itoa(int(len(m.Payload()))),
		}
	}

	// TODO check compressed integrity
	return nil
}

func (ic *IntegrityChecker) scan() (Message, int64, int, error) {
	ok := ic.sc.Scan()
	if ok {
		return Message(ic.sc.Bytes()),
			ic.sc.Offset(),
			ic.sc.ODelta(),
			nil
	}

	if ic.sc.Err() != nil {
		return nil, -1, -1, ic.sc.Err()
	}

	return nil, -1, -1, ErrEndOfTopic
}

// Close releases the underlying resources.
func (ic *IntegrityChecker) Close() error {
	return ic.sc.Close()
}
