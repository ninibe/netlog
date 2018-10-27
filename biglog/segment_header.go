// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"io"
	"time"
)

const (
	headerVersionPos = 0                         // headerVersionPos uint8
	headerLengthPos  = 1                         // headerLengthPos  uint8
	headerCreatedPos = 2                         // headerCreatedPos uint32
	headerSize       = headerCreatedPos + 4 + 10 // 4 bytes of CreatedTS + 10 bytes reserved
)

func readSegHeader(r io.Reader) (segHeader, error) {
	buf := make([]byte, headerSize)
	_, err := r.Read(buf)
	return segHeader(buf), err
}

func newSegHeader() segHeader {
	buf := make([]byte, headerSize)
	buf[headerVersionPos] = 1
	buf[headerLengthPos] = headerSize
	enc.PutUint32(buf[headerCreatedPos:headerCreatedPos+4], uint32(time.Now().Unix()))
	return segHeader(buf)
}

type segHeader []byte

func (sh *segHeader) ver() uint8 {
	return sh.bytes()[headerVersionPos]
}

func (sh *segHeader) len() uint8 {
	return sh.bytes()[headerLengthPos]
}

func (sh *segHeader) createdTS() uint32 {
	return enc.Uint32(sh.bytes()[headerCreatedPos : headerCreatedPos+4])
}

func (sh *segHeader) bytes() []byte {
	return []byte(*sh)
}

func (sh *segHeader) write(w io.Writer) error {
	_, err := w.Write(sh.bytes())
	return err
}
