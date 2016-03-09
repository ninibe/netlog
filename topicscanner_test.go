// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"bytes"
	"testing"
	"time"

	"golang.org/x/net/context"
)

func TestTopicScanner(t *testing.T) {
	t.Parallel()

	nl := tempNetLog()
	topicName := randStr(6)
	topic, err := nl.CreateTopic(topicName, TopicSettings{})
	panicOn(err)

	messages := randMessageSet()
	setLen := len(messages)

	var sequence []byte
	for _, m := range messages {
		sequence = append(sequence, m.Bytes()...)
	}

	// ouput ~ AABCBABCCABC
	var output []Message

	for _, m := range messages {

		// For every message, write the entire set after
		_, err = topic.Write(m)
		panicOn(err)
		_, err = topic.WriteN(sequence, setLen)
		panicOn(err)

		// generate a list with the expected output
		output = append(output, m)
		output = append(output, messages...)
	}

	ts, err := topic.CreateScanner(0)
	panicOn(err)

	ctx, _ := context.WithTimeout(context.Background(), time.Minute)

	for o, m := range output {
		data, offset, err2 := ts.Scan(ctx)
		if err != nil {
			t.Error(err2)
		}

		if int64(o) != offset {
			t.Errorf("Bad scan. Invalid scanned offset %d vs expected %d", offset, o)
		}

		if !bytes.Equal(data, m.Payload()) {
			t.Errorf("Bad scan. Payload not equal to original data.\n Got: % x\n Exp: % x\n", m.Payload(), data)
		}
	}

	// Test embedded offset
	// Offset 3 is embedded in the message-set and should have
	// the same value as message 2 in said message-set, since offset
	ts2, err := topic.CreateScanner(3)
	data, offset, err := ts2.Scan(ctx)
	if err != nil {
		t.Error(err)
	}

	if offset != 3 {
		t.Errorf("Invalid scanned offset %d vs expected %d", offset, 3)
	}

	if !bytes.Equal(data, messages[2].Payload()) {
		t.Errorf("Bad scan. Payload not equal to original data.\n Got: % x\n Exp: % x\n", messages[2].Payload(), data)
	}
}
