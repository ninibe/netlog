// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

func TestCreateSegment(t *testing.T) {
	seg, err := createSegment(os.TempDir(), 128, rand.Int63())
	if err != nil {
		t.Fatal(err)
	}

	mustWrite := func(s string, n uint32) {
		_, err = seg.WriteN([]byte(s), n)
		if err != nil {
			t.Error(err)
		}
	}

	mustWrite("firstsecond", 2)
	mustWrite("third", 1)
	mustWrite("fourth", 1)
	mustWrite("fifth", 1)

	buf := make([]byte, 1000)
	_, err = seg.ReadAt(buf, 0)
	if err != io.EOF {
		t.Error(err)
	}

	if !strings.HasPrefix(string(buf), "firstsecondthirdfourthfifth") {
		t.Errorf("Unexpected read %s \n doesn't start with: %s", buf, "firstsecondthirdfourthfifth")
	}

	err = seg.Delete(true)
	if err != nil {
		t.Error(err)
	}
}

func TestIndexOf(t *testing.T) {
	now := time.Now().Add(-100 * time.Second).Unix()
	seg, err := createSegment(os.TempDir(), 32, rand.Int63())
	if err != nil {
		t.Fatal(err)
	}

	// RO - TS - dFO
	// 1  - 10 - 0
	// 2  - 20 - 100
	// ...
	// 9  - 90 - 800
	var i int
	for i = 0; i < 10; i++ {
		writeEntry(seg.index[i*iw:], uint32(i+1), int64(i*100))
		writeEntryTS(seg.index[i*iw:], uint32(now+int64(i)*10))
	}

	// jump offset for partial RO test
	writeEntry(seg.index[i*iw:], uint32(15), int64(1500))
	writeEntryTS(seg.index[i*iw:], uint32(now+int64(100)))
	i++
	writeEntry(seg.index[i*iw:], uint32(16), int64(1600))
	writeEntryTS(seg.index[i*iw:], uint32(now+int64(100)))

	for i = 0; i < 10; i++ {
		ifo := i * iw

		ro := uint32(i + 1)
		if iro := seg.indexOfRO(ro); iro != ifo {
			t.Errorf("indexOfRO %d was %d expected %d", ro, iro, ifo)
		}

		ts := uint32(now + int64(i)*10)
		if iro := seg.indexOfTS(ts); iro != ifo {
			t.Errorf("indexOfTS %d was %d expected %d", ts, iro, ifo)
		}

		partialTS := uint32(now+int64(i)*10) + 2
		if iro := seg.indexOfTS(partialTS); iro != ifo+iw {
			t.Errorf("indexOfTS %d was %d expected %d", partialTS, iro, ifo+iw)
		}

		dfo := int64(i * 100)
		if iro := seg.indexOfDFO(dfo); iro != ifo {
			t.Errorf("indexOfDFO %d was %d expected %d", dfo, iro, ifo)
		}

		partialDFO := int64(i*100) + 5
		if iro := seg.indexOfDFO(partialDFO); iro != ifo {
			t.Errorf("indexOfDFO %d was %d expected %d", partialDFO, iro, ifo)
		}
	}

	// since 14 does not reach 15, should fall back to the last value of the loop
	i--
	partialRO := uint32(14)
	if iro := seg.indexOfRO(partialRO); iro != i*iw {
		t.Errorf("indexOfRO %d was %d expected %d", partialRO, iro, i*iw)
	}

	i++
	RO := uint32(15)
	if iro := seg.indexOfRO(RO); iro != i*iw {
		t.Errorf("indexOfRO %d was %d expected %d", RO, iro, i*iw)
	}
}

func TestHealthCheckPartialWrite(t *testing.T) {
	rand.Seed(int64(time.Now().Nanosecond()))
	seg, err := createSegment(os.TempDir(), 128, rand.Int63())
	panicOn(err)
	defer logDelete(seg, true)

	_, err = seg.WriteN([]byte("some"), 1)
	panicOn(err)
	_, err = seg.WriteN([]byte("test"), 2)
	panicOn(err)
	_, err = seg.WriteN([]byte("data"), 1)
	panicOn(err)

	_, err = seg.write([]byte("bypassing the index update"))
	panicOn(err)

	_, err = seg.dataFile.Seek(0, 0)
	panicOn(err)

	data, err := ioutil.ReadAll(seg.dataFile)
	if string(data) != "sometestdatabypassing the index update" {
		t.Fatalf("can not test HealthCheckPartialWrite, data: %s", data)
	}

	err = seg.healthCheckPartialWrite()
	if err != nil {
		t.Fatal(err)
	}

	_, err = seg.dataFile.Seek(0, 0)
	panicOn(err)

	data, err = ioutil.ReadAll(seg.dataFile)
	if string(data) != "sometestdata" {
		t.Errorf("data file not corrected from partial write, data: %s", data)
	}
}
