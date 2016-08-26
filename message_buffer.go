// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"log"
	"sync"
	"time"

	"github.com/ninibe/netlog/message"
)

type nWriter interface {
	WriteN(p []byte, n int) (written int, err error)
}

type messageBuffer struct {
	writer nWriter
	comp   message.CompressionType

	mu       sync.Mutex
	buff     []message.Message
	buffered int
	messages int
	stopChan chan struct{}
}

func newMessageBuffer(w nWriter, settings TopicSettings) *messageBuffer {

	m := &messageBuffer{
		writer:   w,
		buff:     make([]message.Message, settings.BatchNumMessages),
		comp:     settings.CompressionType,
		messages: settings.BatchNumMessages,
		stopChan: make(chan struct{}),
	}

	go m.launchFlusher(settings.BatchInterval.Duration())

	return m
}

// Write implements the io.Writer interface for the messageBuffer
func (m *messageBuffer) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.buff[m.buffered] = message.Message(p)
	m.buffered++
	if m.buffered == m.messages {
		err = m.flush()
	}

	return len(p), err
}

// Flush flushes the data from the buffer into the underlying writer
func (m *messageBuffer) Flush() (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.flush()
}

func (m *messageBuffer) flush() (err error) {
	if m.buffered == 0 {
		return nil
	}

	defer func() {
		m.buffered = 0
	}()

	var data message.Message
	if m.buffered == 1 {
		data = m.buff[0]
	} else {
		data = message.Pack(m.buff[:m.buffered], m.comp)
	}

	_, err = m.writer.WriteN(data.Bytes(), m.buffered)
	return err
}

// Close releases all resources
func (m *messageBuffer) Close() (err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stopChan <- struct{}{}
	m.writer = nil
	return nil
}

func (m *messageBuffer) launchFlusher(d time.Duration) {
	if d == 0 {
		return
	}

	ticker := time.NewTicker(d)
	for {
		select {
		case <-ticker.C:
			if err := m.Flush(); err != nil {
				log.Printf("alert: flush failed: %s", err)
			}
			continue
		case <-m.stopChan:
			close(m.stopChan)
			return
		}
	}
}
