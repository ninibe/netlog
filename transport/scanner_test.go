// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/ninibe/netlog"
)

func TestScanner(t *testing.T) {
	ts := runTestHTTPServer()

	// CREATE TOPIC
	topicURL := fmt.Sprintf("%s/scanner_test", ts.URL)
	req, err := http.NewRequest("POST", topicURL, nil)
	panicOn(err)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
	}
	_, _ = io.Copy(os.Stdout, r.Body)

	postURL := fmt.Sprintf("%s/scanner_test/payload", ts.URL)

	// WRITE DATA
	data := randDataSet(100, 1024)
	for k := range data {
		req2, err2 := http.NewRequest("POST", postURL, bytes.NewBuffer(data[k]))
		panicOn(err2)
		r2, err2 := http.DefaultClient.Do(req2)
		if err2 != nil {
			t.Error(err2)
		}
		_, _ = io.Copy(os.Stdout, r2.Body)
	}

	// CREATE SCANNER
	scannerURL := fmt.Sprintf("%s/scanner_test/scanner?from=0", ts.URL)
	req, err = http.NewRequest("POST", scannerURL, nil)
	panicOn(err)
	r, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
	}

	si := netlog.TScannerInfo{}
	dec := json.NewDecoder(r.Body)
	err = dec.Decode(&si)
	if err != nil {
		t.Fatal(err)
	}

	scanURL := fmt.Sprintf("%s/scanner_test/scan?id=%s", ts.URL, si.ID)

	for k := range data {
		req, err = http.NewRequest("GET", scanURL, nil)
		panicOn(err)
		r, err = http.DefaultClient.Do(req)
		if err != nil {
			t.Error(err)
			continue
		}

		if r.StatusCode != 200 {
			t.Errorf("Scanner received status code %d", r.StatusCode)
			continue
		}

		payload, err2 := ioutil.ReadAll(r.Body)
		if err2 != nil {
			t.Error(err2)
			continue
		}

		if !bytes.Equal(data[k], payload) {
			t.Errorf("payload read error:\n Expected: % s\n Actual: % s", data[k], payload)
		}
	}

	// TODO test concurrent access

	topicURL = fmt.Sprintf("%s/scanner_test", ts.URL)
	req, err = http.NewRequest("DELETE", topicURL, nil)
	panicOn(err)
	_, err = http.DefaultClient.Do(req)
	panicOn(err)
}
