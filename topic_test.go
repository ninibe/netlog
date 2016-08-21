// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"testing"
	"time"
)

func TestTopicCreateGetDelete(t *testing.T) {
	t.Parallel()

	nl := tempNetLog()
	topicName := randStr(6)

	top, err := nl.Topic(topicName)
	if err != ErrTopicNotFound || top != nil {
		t.Error("Non existing topic should have been not found")
	}

	top, err = nl.CreateTopic(topicName, TopicSettings{})
	if err != nil {
		t.Fatal(err)
	}

	top2, err := nl.Topic(topicName)
	if err != nil {
		t.Error(err)
	}

	if top != top2 {
		t.Error("failed to retrieve topic by name")
	}

	err = nl.DeleteTopic(topicName, false)
	if err != nil {
		t.Error(err)
	}

	top, err = nl.Topic(topicName)
	if err != ErrTopicNotFound || top != nil {
		t.Error("Invalid topic should have been not found")
	}
}

func TestParseOffset(t *testing.T) {
	t.Parallel()

	var parseTests = []struct {
		str    string
		offset int64
		err    error
	}{
		{"3", 3, nil},
		{"first", 0, nil},
		{"start", 0, nil},
		{"beginning", 0, nil},
		{"oldest", 0, nil},
		{"last", 19, nil},
		{"latest", 19, nil},
		{"end", 20, nil},
		{"now", 20, nil},
		{"", 0, nil},
		{"2a2", -1, ErrInvalidOffset},
		{"1s", 10, nil},
		{"10s", 0, nil},
	}

	nl := tempNetLog()
	top, err := nl.CreateTopic(randStr(6), TopicSettings{})
	panicOn(err)

	for i := 1; i <= 10; i++ {
		_, err := top.Write(randData(1000))
		panicOn(err)
	}

	time.Sleep(2 * time.Second)

	for i := 1; i <= 10; i++ {
		_, err := top.Write(randData(1000))
		panicOn(err)
	}

	err = top.bl.Sync()
	panicOn(err)

	for _, tt := range parseTests {
		offset, err := top.ParseOffset(tt.str)
		if offset != tt.offset {
			t.Errorf("invalid parsed offset %q Expected: %d Actual: %d", tt.str, tt.offset, offset)
		}

		if err != tt.err {
			t.Errorf("invalid parsed offset error %q Expected: %s Actual: %s", tt.str, tt.err, err)
		}
	}
}
