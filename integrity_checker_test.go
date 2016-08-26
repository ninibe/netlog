// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/ninibe/netlog/message"
)

func TestTopicIntegrity(t *testing.T) {
	t.Parallel()

	nl := tempNetLog()
	topic, err := nl.CreateTopic("corrupt", TopicSettings{})
	panicOn(err)
	msgs := randMessageSet()

	for k := range msgs {
		if k == 4 {
			// corrupt length header for offset 4
			enc.PutUint32(msgs[k][message.PosPLenth:message.PosPLenth+4], msgs[k].PLength()+2)
		}

		if k == 7 {
			// corrupt crc header for offset 7
			enc.PutUint32(msgs[k][message.PosCRC32:message.PosCRC32+4], uint32(20))
		}

		_, err2 := topic.Write(msgs[k])
		panicOn(err2)
	}

	iErrs, err := topic.CheckIntegrity(context.Background(), 0)
	panicOn(err)

	if len(iErrs) != 2 {
		t.Errorf("Expected %d integrity errors. Found %d.", 2, len(iErrs))
	}

	if iErrs[0].Type != message.IntegrityLengthErr {
		t.Errorf("Expected error type %s actual %s", message.IntegrityLengthErr, iErrs[0].Type)
	}

	if iErrs[1].Type != message.IntegrityChecksumErr {
		t.Errorf("Expected error type %s actual %s", message.IntegrityChecksumErr, iErrs[0].Type)
	}

	if iErrs[0].Offset != 4 {
		t.Errorf("Expected error on offset %d got %d", 4, iErrs[0].Offset)
	}

	if iErrs[1].Offset != 7 {
		t.Errorf("Expected error on offset %d got %d", 7, iErrs[1].Offset)
	}
}
