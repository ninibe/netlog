// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/ninibe/netlog/biglog"
	"github.com/ninibe/netlog/message"
)

// NLError is a known NetLog error with an associates status code.
type NLError interface {
	Error() string
	String() string
	StatusCode() int
}

type nlError struct {
	OK     bool        `json:"ok"`
	Status int         `json:"status"`
	Err    string      `json:"error"`
	Reason interface{} `json:"reason,omitempty"`
}

func newErr(status int, message string) NLError {
	return &nlError{false, status, message, nil}
}

// StatusCode used by the http transport.
func (e *nlError) StatusCode() int {
	return e.Status
}

// Error returns the error string.
func (e *nlError) Error() string {
	return e.Err
}

// String implements the Stringer interface for NLError.
func (e *nlError) String() string {
	return fmt.Sprintf("nlerror: %s", e.Err)
}

var (
	// ErrUnknown is returned when an underlying stardard Go error reaches the user.
	ErrUnknown = newErr(http.StatusInternalServerError, "netlog: unkwown error")
	// ErrInvalidDir is returned when the data folder provided does not exists or is not writable.
	ErrInvalidDir = newErr(http.StatusInternalServerError, "netlog: invalid data directory")

	// ErrBadRequest is returned when invalid parameters are received.
	ErrBadRequest = newErr(http.StatusBadRequest, "netlog: bad request")
	// ErrInvalidOffset is returned when the requested offset can not be parsed into an number.
	ErrInvalidOffset = newErr(http.StatusBadRequest, "netlog: invalid offset")
	// ErrInvalidDuration is returned when a given big duration can not be parsed
	ErrInvalidDuration = newErr(http.StatusBadRequest, "netlog: invalid duration")
	// ErrInvalidCompression is returning when the compression type defined is unknown
	ErrInvalidCompression = newErr(http.StatusBadRequest, "netlog: invalid compression type")
	// ErrTopicExists is returning when trying to create an already existing topic.
	ErrTopicExists = newErr(http.StatusBadRequest, "netlog: topic exists")
	// ErrEndOfTopic is returned when the reader has read all the way until the end of the topic.
	ErrEndOfTopic = newErr(http.StatusNotFound, "netlog: end of topic")
	// ErrTopicNotFound is returned when addressing an non-existing topic.
	ErrTopicNotFound = newErr(http.StatusNotFound, "netlog: topic not found")

	// ErrScannerNotFound is returning when using a non-existing scanner ID.
	ErrScannerNotFound = newErr(http.StatusNotFound, "netlog: scanner not found")
	// ErrOffsetNotFound is returning when the offset is no longer or not yet present in the topic.
	ErrOffsetNotFound = newErr(http.StatusNotFound, "netlog: offset not found")

	// ErrCRC is returned when a message's payload does not match's the CRC header.
	ErrCRC = newErr(http.StatusInternalServerError, "netlog: checksum error")
	// ErrBusy is retuning when trying to close or delete a topic with readers attached to it.
	ErrBusy = newErr(http.StatusConflict, "netlog: resource busy")
)

var errmap = map[error]NLError{
	biglog.ErrBusy:         ErrBusy,
	biglog.ErrNotFound:     ErrOffsetNotFound,
	io.EOF:                 ErrEndOfTopic,
	message.ErrCompression: ErrInvalidCompression,
}

// ExtErr maps external errors, mostly BigLog errors to NetLog errors.
func ExtErr(err error) NLError {

	// If it's not really external return same error
	if err, ok := err.(NLError); ok {
		return err.(NLError)
	}

	// map to corresponding NetLog error
	if nlerr, ok := errmap[err]; ok {
		return nlerr
	}

	log.Printf("error: unmapped error: %s", err.Error())

	// Error is unknown
	return ErrUnknown
}
