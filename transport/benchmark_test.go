// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package transport

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
)

var data = []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam volutpat ante in rhoncus commodo.
Morbi id ipsum rutrum, elementum tortor ac, pellentesque libero. Vestibulum tincidunt porta orci.
Vestibulum mattis, augue non mattis malesuada, lectus justo iaculis ipsum, eget euismod sapien nunc vitae nisi.
Nullam eu mattis dui. Etiam faucibus egestas erat sit amet semper. Praesent tincidunt mattis blandit.
Ut sed magna eget lacus ullamcorper semper sed quis felis. Aenean id consequat orci. In hac habitasse platea dictumst.
Nam tincidunt ipsum vitae egestas sollicitudin.`)

type dataReader struct {
	data []byte
}

func (d dataReader) Read(p []byte) (n int, err error) {
	max := len(d.data)
	for k := range p {
		if k == max {
			return k, err
		}
		p[k] = d.data[k]
	}

	return len(p), nil
}

func BenchmarkWritePayload(b *testing.B) {
	ts := runTestHTTPServer()
	defer ts.Close()

	topicURL := fmt.Sprintf("%s/bench1", ts.URL)
	postURL := fmt.Sprintf("%s/bench1/payload", ts.URL)
	req, err := http.NewRequest("DELETE", topicURL, nil)
	panicOn(err)
	r, err := http.DefaultClient.Do(req)
	panicOn(err)
	_, _ = io.Copy(os.Stdin, r.Body)

	req, err = http.NewRequest("POST", topicURL, nil)
	panicOn(err)
	r, err = http.DefaultClient.Do(req)
	panicOn(err)
	_, _ = io.Copy(os.Stdin, r.Body)

	data = bytes.Repeat(data, 20)
	buf := bytes.NewBuffer(data)
	_, err = http.NewRequest("POST", topicURL, buf)
	panicOn(err)

	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		buf := bytes.NewBuffer(data)
		req, err = http.NewRequest("POST", postURL, buf)
		panicOn(err)
		r, err = http.DefaultClient.Do(req)
		panicOn(err)
		_, _ = io.Copy(os.Stdout, r.Body)
	}
	b.StopTimer()

	topicURL = fmt.Sprintf("%s/bench1", ts.URL)
	req, err = http.NewRequest("DELETE", topicURL, nil)
	panicOn(err)
	_, err = http.DefaultClient.Do(req)
	panicOn(err)
}
