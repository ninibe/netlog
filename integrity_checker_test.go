package netlog

import (
	"testing"

	"golang.org/x/net/context"
)

func TestCheckMessageIntegrity(t *testing.T) {
	t.Parallel()

	data := []byte("aaaaaaaaaa")
	m := MessageFromPayload(data)

	if CheckMessageIntegrity(m, 1) != nil {
		t.Errorf("Integrity check failed with valid message")
	}

	// corrupt length header
	enc.PutUint32(m[plenthPos:plenthPos+4], uint32(20))
	i1 := CheckMessageIntegrity(m, 1)
	if i1 == nil {
		t.Fatalf("Integrity check passed with invalid message")
	}

	if i1.Type != IntegrityLengthErr {
		t.Errorf("Invalid integrity error type, expected %s vs actual %s", IntegrityLengthErr, i1.Type)
	}

	if i1.ODelta != 1 || i1.Offset != 0 {
		t.Errorf("Invalid offset/delta returned by integrity checker: offset: %d delta: %d", i1.Offset, i1.ODelta)
	}

	// restore correct length
	enc.PutUint32(m[plenthPos:plenthPos+4], uint32(len(data)))
	// corrupt crc header
	enc.PutUint32(m[crc32Pos:crc32Pos+4], uint32(20))

	i2 := CheckMessageIntegrity(m, 1)
	if i2 == nil {
		t.Fatalf("Integrity check passed with invalid message")
	}

	if i2.Type != IntegrityChecksumErr {
		t.Errorf("Invalid integrity error type, expected %s vs actual %s", IntegrityChecksumErr, i2.Type)
	}
}

func TestTopicIntegrity(t *testing.T) {
	t.Parallel()

	nl := tempNetLog()
	topic, err := nl.CreateTopic("corrupt", TopicSettings{})
	panicOn(err)
	msgs := randMessageSet()

	for k := range msgs {
		if k == 4 {
			// corrupt length header for offset 4
			enc.PutUint32(msgs[k][plenthPos:plenthPos+4], msgs[k].PLength()+2)
		}

		if k == 7 {
			// corrupt crc header for offset 7
			enc.PutUint32(msgs[k][crc32Pos:crc32Pos+4], uint32(20))
		}

		_, err2 := topic.Write(msgs[k])
		panicOn(err2)
	}

	iErrs, err := topic.CheckIntegrity(context.Background(), 0)
	panicOn(err)

	if len(iErrs) != 2 {
		t.Errorf("Expected %d integrity errors. Found %d.", 2, len(iErrs))
	}

	if iErrs[0].Type != IntegrityLengthErr {
		t.Errorf("Expected error type %s actual %s", IntegrityLengthErr, iErrs[0].Type)
	}

	if iErrs[1].Type != IntegrityChecksumErr {
		t.Errorf("Expected error type %s actual %s", IntegrityChecksumErr, iErrs[0].Type)
	}

	if iErrs[0].Offset != 4 {
		t.Errorf("Expected error on offset %d got %d", 4, iErrs[0].Offset)
	}

	if iErrs[1].Offset != 7 {
		t.Errorf("Expected error on offset %d got %d", 7, iErrs[1].Offset)
	}
}
