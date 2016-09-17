// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlogc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// NewClient returns a NetLog client connecting to a given server address
func NewClient(addr string) *Client {
	return &Client{
		addr:  addr,
		httpc: &http.Client{Timeout: 30 * time.Second},
	}
}

// Client is a NetLog client
type Client struct {
	addr  string
	httpc *http.Client
}

// CreateTopic creates a new topic with a given name.
// It will return an error if the topic already exist.
func (c *Client) CreateTopic(name string) (*Topic, error) {
	resp, err := c.httpc.Post(c.topicURL(name), "application/json", nil)
	if err != nil {
		return nil, err
	}

	defer logClose(resp.Body)
	if resp.StatusCode == http.StatusCreated {
		return &Topic{
			name:   name,
			client: c,
		}, nil
	}

	return nil, decodeError(resp.Body)
}

// Topic returns a Topic by a given name.
// The existence of the topic is not checked,
// further operations to a non-existing topic will fail instead.
func (c *Client) Topic(name string) *Topic {
	return &Topic{
		name:   name,
		client: c,
	}
}

func (c *Client) topicURL(name string) string {
	return fmt.Sprintf("%s/%s", c.addr, name)
}

// Topic represents a NetLog topic, a form of namespace.
type Topic struct {
	name   string
	client *Client
}

func (t *Topic) url() string {
	return t.client.topicURL(t.name)
}

// Write writes p into a single entry in the topic.
// Implements io.Reader interface.
func (t *Topic) Write(p []byte) (int, error) {
	n, err := t.ReadFrom(bytes.NewReader(p))
	return int(n), err
}

// ReadFrom reads from r until io.EOF and writes a single entry to the topic with the data.
func (t *Topic) ReadFrom(r io.Reader) (int64, error) {
	endPoint := fmt.Sprintf("%s/payload", t.url())
	req, err := http.NewRequest("POST", endPoint, r)
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := t.client.httpc.Do(req)
	if err != nil {
		return 0, err
	}

	defer logClose(resp.Body)

	if resp.StatusCode == http.StatusCreated {
		return req.ContentLength, nil
	}

	return 0, decodeError(resp.Body)
}

// TopicScanner reads one by one over the messages in a topic
// blocking until new data is available for a period of time.
type TopicScanner struct {
	ID      string `json:"id"`
	From    int64  `json:"from"`
	Last    int64  `json:"last"`
	Persist bool   `json:"persist"`

	topic *Topic
	buf   []byte
	off   int64
	err   error
}

// NewScanner creates a new scanner starting at offset `from`. If `persist` is true,
// the scanner and it's state will survive server restarts
func (t *Topic) NewScanner(from string, persist bool) (*TopicScanner, error) {

	endPoint := fmt.Sprintf("%s/scanner", t.url())
	req, err := http.NewRequest("POST", endPoint, nil)
	if err != nil {
		return nil, err
	}

	params := url.Values{}
	params.Add("from", from)
	params.Add("persist", fmt.Sprintf("%t", persist))
	req.URL.RawQuery = params.Encode()

	resp, err := t.client.httpc.Do(req)
	if err != nil {
		return nil, err
	}

	defer logClose(resp.Body)

	if resp.StatusCode == http.StatusCreated {
		ts := &TopicScanner{}
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(ts); err != nil {
			return nil, errors.New("Unknown server response")
		}

		ts.topic = t
		return ts, nil
	}

	return nil, decodeError(resp.Body)
}

// Scan advances the Scanner to the next message, returning the message and the offset.
// Scan will block when it reaches EOF until there is more data available for a time defined by `wait`.
// `wait` takes text forms of time like "1day2h5s"
func (ts *TopicScanner) Scan(wait string) bool {

	endPoint := fmt.Sprintf("%s/scan", ts.topic.url())
	req, err := http.NewRequest("GET", endPoint, nil)
	if err != nil {
		ts.err = err
		return false
	}

	params := url.Values{}
	params.Add("id", ts.ID)
	if wait != "" {
		params.Add("wait", wait)
	}
	req.URL.RawQuery = params.Encode()

	resp, err := ts.topic.client.httpc.Do(req)
	if err != nil {
		ts.err = err
		return false
	}

	defer logClose(resp.Body)
	if resp.StatusCode != http.StatusOK {
		ts.err = decodeError(resp.Body)
		return false
	}

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		ts.err = err
		return false
	}

	ts.buf = buf
	ts.off, err = strconv.ParseInt(resp.Header.Get("X-Offset"), 10, 0)
	if err != nil {
		ts.err = err
		return false
	}

	return true
}

// Bytes returns content of the scanned entry.
func (ts *TopicScanner) Bytes() []byte {
	return ts.buf
}

// Offset returns the offset of the scanned entry.
func (ts *TopicScanner) Offset() int64 {
	return ts.off
}

// Err returns the first non-EOF error that was encountered by the Scanner.
func (ts *TopicScanner) Err() error {
	if ts.err == io.EOF {
		return nil
	}

	return ts.err
}

func decodeError(r io.ReadCloser) error {
	nlerr := &NLError{}
	dec := json.NewDecoder(r)
	if err := dec.Decode(nlerr); err != nil {
		return errors.New("Unknown server error response")
	}

	return nlerr
}

// NLError is an error message returned by a NetLog server
type NLError struct {
	OK     bool   `json:"ok"`
	Status int    `json:"status"`
	Err    string `json:"error"`
}

// Error returns the error string.
func (e *NLError) Error() string {
	return e.Err
}

// logClose calls Close on the subject and logs the error if any
// this is handy to call Close on defer
func logClose(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Printf("error: %s", err)
	}
}
