// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package message

import "testing"

func TestCheckMessageIntegrity(t *testing.T) {
	t.Parallel()

	data := []byte("aaaaaaaaaa")
	m := MessageFromPayload(data)

	if CheckMessageIntegrity(m, 1) != nil {
		t.Errorf("Integrity check failed with valid message")
	}

	// corrupt length header
	enc.PutUint32(m[PosPLenth:PosPLenth+4], uint32(20))
	i1 := CheckMessageIntegrity(m, 1)
	if i1 == nil {
		t.Fatal("Integrity check passed with invalid message")
	}

	if i1.Type != IntegrityLengthErr {
		t.Errorf("Invalid integrity error type, expected %s vs actual %s", IntegrityLengthErr, i1.Type)
	}

	if i1.ODelta != 1 || i1.Offset != 0 {
		t.Errorf("Invalid offset/delta returned by integrity checker: offset: %d delta: %d", i1.Offset, i1.ODelta)
	}

	// restore correct length
	enc.PutUint32(m[PosPLenth:PosPLenth+4], uint32(len(data)))
	// corrupt crc header
	enc.PutUint32(m[PosCRC32:PosCRC32+4], uint32(20))

	i2 := CheckMessageIntegrity(m, 1)
	if i2 == nil {
		t.Fatal("Integrity check passed with invalid message")
	}

	if i2.Type != IntegrityChecksumErr {
		t.Errorf("Invalid integrity error type, expected %s vs actual %s", IntegrityChecksumErr, i2.Type)
	}
}
