// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package transport

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"testing"
)

func TestSetGetPayload(t *testing.T) {
	ts := runTestHTTPServer()

	// CREATE TOPIC
	topicURL := fmt.Sprintf("%s/test", ts.URL)
	req, err := http.NewRequest("POST", topicURL, nil)
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
	}
	_, _ = io.Copy(os.Stdout, r.Body)

	postURL := fmt.Sprintf("%s/test/payload", ts.URL)

	// WRITE DATA
	data := randDataSet(100, 1024)
	for k := range data {
		req2, err2 := http.NewRequest("POST", postURL, bytes.NewBuffer(data[k]))
		panicOn(err2)
		r, err2 := http.DefaultClient.Do(req2)
		if err2 != nil {
			t.Error(err2)
		}
		_, _ = io.Copy(os.Stdout, r.Body)
	}

	// READ BACK
	var readers = 10
	var wg sync.WaitGroup
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(data [][]byte) {
			for k := range data {
				offsetURL := fmt.Sprintf("%s/test/payload/%d", ts.URL, k)
				req3, err3 := http.NewRequest("GET", offsetURL, nil)
				panicOn(err3)
				r, err3 := http.DefaultClient.Do(req3)
				if err3 != nil {
					t.Error(err3)
				}

				payload, err3 := ioutil.ReadAll(r.Body)
				if err3 != nil {
					t.Error(err3)
				}

				if !bytes.Equal(data[k], payload) {
					t.Errorf("payload read error:\n Expected: % x\n Actual: % x", data[k], payload)
				}
			}
			wg.Done()
		}(data)
	}

	wg.Wait()
	topicURL = fmt.Sprintf("%s/test", ts.URL)
	req, err = http.NewRequest("DELETE", topicURL, nil)
	_, err = http.DefaultClient.Do(req)
	panicOn(err)
}
