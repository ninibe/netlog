// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package message

import (
	"bytes"
	"hash/crc32"
	"math/rand"
	"testing"

	"github.com/golang/snappy"
)

func TestMessage(t *testing.T) {
	t.Parallel()
	data := randData(rand.Intn(990) + 10)
	msg := FromPayload(data)
	testMessage(t, data, msg)
}

func testMessage(t *testing.T, data []byte, msg Message) {
	dlen := len(data)

	if len(msg.Bytes()) != dlen+headerSize {
		t.Errorf("Bad Message. Invalid message length from payload: %d vs expected %d", len(msg.Bytes()), dlen+headerSize)
	}

	if int(msg.PLength()) != dlen {
		t.Errorf("Bad Message. Invalid payload length: %d vs expected %d", msg.PLength(), dlen)
	}

	crc := crc32.ChecksumIEEE(data)
	if crc != msg.CRC32() {
		t.Errorf("Bad Message. Invalid CRC32: %d vs expected %d", crc, msg.CRC32())
	}

	if !msg.ChecksumOK() {
		t.Error("Bad Message. Self checksum failed.")
	}

	if !bytes.Equal(data, msg.Payload()) {
		t.Errorf("Bad Message. Payload not equal to original data.\n Got: % x\n Exp: % x\n", msg.Payload(), data)
	}
}

func TestUnpackSequence(t *testing.T) {
	t.Parallel()

	messages := randMessageSet()
	var sequence []byte
	for _, m := range messages {
		sequence = append(sequence, m.Bytes()...)
	}

	unpacked, err := Unpack(sequence)
	if err != nil {
		t.Error(err)
	}

	if len(unpacked) != len(messages) {
		t.Errorf("Unpacked %d messages vs expected %d", len(unpacked), len(messages))
	}

	for k, m := range messages {
		testMessage(t, m.Payload(), unpacked[k])
	}
}

func TestUnpackGzip(t *testing.T) {
	t.Parallel()

	msgs := randMessageSet()
	set := Pack(msgs, CompressionGzip)

	if set.Compression() != CompressionGzip {
		t.Errorf("Missing gzip flag, got %d", set.Compression())
	}

	unpacked, err := Unpack(set)
	if err != nil {
		t.Error(err)
	}

	if len(unpacked) != len(msgs) {
		t.Errorf("Unpacked %d messages vs expected %d", len(unpacked), len(msgs))
	}

	for k, m := range msgs {
		testMessage(t, m.Payload(), unpacked[k])
	}
}

func TestUnpackSnappy(t *testing.T) {
	t.Parallel()

	msgs := randMessageSet()
	set := Pack(msgs, CompressionSnappy)

	if set.Compression() != CompressionSnappy {
		t.Errorf("Missing snappy flag, got %d", set.Compression())
	}

	if _, err := snappy.DecodedLen(set.Payload()); err != nil {
		t.Errorf("Set's payload does not look like snappy: %s", err)
	}

	unpacked, err := Unpack(set)
	if err != nil {
		t.Error(err)
	}

	if len(unpacked) != len(msgs) {
		t.Errorf("Unpacked %d messages vs expected %d", len(unpacked), len(msgs))
	}

	for k, m := range msgs {
		testMessage(t, m.Payload(), unpacked[k])
	}
}
