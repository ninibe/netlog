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
	"log"
	"net/http"
	"time"
)

func NewClient(addr string) *Client {
	return &Client{
		addr:  addr,
		httpc: &http.Client{Timeout: 30 * time.Second},
	}
}

type Client struct {
	addr  string
	httpc *http.Client
}

func (c *Client) CreateTopic(name string) (*Topic, error) {
	resp, err := c.httpc.Post(c.topicURL(name), "application/json", nil)
	if err != nil {
		return nil, err
	}

	defer logClose(resp.Body)
	if resp.StatusCode == 201 {
		return &Topic{
			name:   name,
			client: c,
		}, nil
	}

	return nil, decodeError(resp.Body)
}

func (c *Client) Topic(name string) *Topic {
	return &Topic{
		name:   name,
		client: c,
	}
}

func (c *Client) topicURL(name string) string {
	return fmt.Sprintf("%s/%s", c.addr, name)
}

type Topic struct {
	name   string
	client *Client
}

func (t *Topic) url() string {
	return t.client.topicURL(t.name)
}

func (t *Topic) Write(p []byte) (int, error) {
	return t.ReadFrom(bytes.NewReader(p))
}

func (t *Topic) ReadFrom(r io.Reader) (int64, error) {
	url := fmt.Sprintf("%s/payload", t.url())
	req, err := http.NewRequest("POST", url, r)
	if err != nil {
		return 0, err
	}

	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := t.client.httpc.Do(req)
	if err != nil {
		return 0, err
	}

	defer logClose(resp.Body)

	if resp.StatusCode == 201 {
		return req.ContentLength, nil
	}

	return req.ContentLength, decodeError(resp.Body)
}

type TopicScanner struct {
	t  *Topic
	id string
}

// NewScanner creates a new scanner starting at offset `from`. If `persist` is true,
// the scanner and it's state will survive server restarts
func (t *Topic) NewScanner(from int64, persist bool) (*TopicScanner, error) {
	req, err := http.NewRequest("POST", t.url(), nil)
	if err != nil {
		return err
	}

	resp, err := t.client.httpc.Do(req)
	if err != nil {
		return nil, err
	}

	defer logClose(resp.Body)

	if resp.StatusCode == 201 {
		ts := &TopicScanner{}
		dec := json.NewDecoder(resp.Body)
		if err := dec.Decode(ts); err != nil {
			return nil, errors.New("Unknown server response")
		}

		ts.t = t
		return ts, nil
	}

	return nil, decodeError(resp.Body)
}

func (ts *TopicScanner) Scan() bool {
	resp, err := ts.t.client.httpc.Get(ts.t.url())
	if err != nil {
		return nil, err
	}

	defer logClose(resp.Body)
	if resp.StatusCode != 200 {
		return nil, decodeError(resp.Body)
	}

	return nil, decodeError(resp.Body)
}

func decodeError(r io.ReadCloser) error {
	nlerr := &NLError{}
	dec := json.NewDecoder(r)
	if err := dec.Decode(nlerr); err != nil {
		return errors.New("Unknown server error response")
	}

	return nlerr
}

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
