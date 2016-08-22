// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog_test

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ninibe/netlog/biglog"
)

func TestNewBigLog(t *testing.T) {

	bl, err := biglog.Create(filepath.Join(os.TempDir(), fmt.Sprintf("biglogtest-%d", rand.Int63())), 100)
	bl.SetOpts(biglog.BufioWriter(6))

	if err != nil {
		t.Fatal(err)
	}

	_, err = bl.Write([]byte("first")) //5
	if err != nil {
		t.Error(err)
	}

	_, err = bl.Write([]byte("second")) // 11
	if err != nil {
		t.Error(err)
	}

	_, err = bl.Write([]byte("third")) // 16
	if err != nil {
		t.Error(err)
	}

	_, err = bl.Write([]byte("fourth")) // 22
	if err != nil {
		t.Error(err)
	}

	buf := make([]byte, 11)

	r, _, _ := biglog.NewReader(bl, 1) // -5

	_, err = r.Read(buf) // -16
	if err != nil {
		t.Error(err)
	}

	err = bl.Trim()
	if err != biglog.ErrLastSegment {
		t.Error(err)
	}

	if !strings.HasPrefix(string(buf), "secondthird") {
		t.Errorf("Unexpected read %s \n doesn't start with: %s", buf, "third")
	}

	err = bl.Split()
	if err != nil {
		t.Error(err)
	}

	info, _ := bl.Info()
	if len(info.Segments) != 2 {
		t.Errorf("Segment cut failed, found %d segments", len(info.Segments))
	}

	_, err = bl.Write([]byte("fifth")) // 27 - 0
	if err != nil {
		t.Error(err)
	}

	err = bl.Trim()
	if err != biglog.ErrSegmentBusy {
		t.Error(err)
	}

	buf = make([]byte, 6)
	_, err = r.Read(buf)
	if err != nil {
		t.Error(err)
	}

	if !strings.HasPrefix(string(buf), "fourth") {
		t.Errorf("Unexpected read %s \n doesn't start with: %s", buf, "fourth")
	}

	buf = make([]byte, 6)
	_, err = r.Read(buf)
	if err != io.EOF {
		t.Error(err)
	}

	// This data should sill be in the io buffer
	if string(buf) != "\x00\x00\x00\x00\x00\x00" {
		t.Errorf("Unflushed data returned %q", buf)
	}

	err = bl.Sync()
	if err != nil {
		t.Error(err)
	}

	_, err = r.Read(buf)
	if err != io.EOF {
		t.Error(err)
	}

	if !strings.HasPrefix(string(buf), "fifth") {
		t.Errorf("Unexpected read %q \n doesn't start with: %s", buf, "fifth")
	}

	err = bl.Split()
	if err != nil {
		t.Error(err)
	}

	info, _ = bl.Info()
	if len(info.Segments) != 3 {
		t.Errorf("Segment split failed, found %d segments", len(info.Segments))
	}

	_, err = bl.WriteN([]byte("sith"), 3) // 31 - 0
	if err != nil {
		t.Error(err)
	}

	err = bl.Sync()
	if err != nil {
		t.Error(err)
	}

	r, _, _ = biglog.NewReader(bl, 0)
	buf = make([]byte, 100)
	_, err = r.Read(buf)
	if err != io.EOF {
		t.Error(err)
	}

	if !strings.HasPrefix(string(buf), "firstsecondthirdfourthfifthsith") {
		t.Errorf("Unexpected read %s \n doesn't start with: %s", buf, "firstsecondthirdfourthfifthsith")
	}

	sc, err := biglog.NewScanner(bl, 0, biglog.UseBuffer(make([]byte, 10)))
	for i := 0; i < 10; i++ {
		if !sc.Scan() {
			if i != 6 {
				t.Errorf("Unexpected end of scan in iteration %d", i)
			}
			break
		}

		biglog.Logger.Printf("scanner %d = %s \n", i, sc.Bytes())
	}

	err = bl.Trim()
	if err != nil {
		t.Error(err)
	}

	info, _ = bl.Info()
	if len(info.Segments) != 2 {
		t.Errorf("Segment delete failed, found %d segments", len(info.Segments))
	}

	err = bl.Delete(true)
	if err != nil {
		t.Error(err)
	}
}
