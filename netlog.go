// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/ninibe/bigduration"
	"github.com/ninibe/netlog/biglog"
)

// Option is the type of function used to set internal parameters.
type Option func(*NetLog)

// NetLog is the main struct that serves a set of topics, usually
// it must be wrapped with an HTTP transport.
type NetLog struct {
	dataDir       string
	topics        *TopicAtomicMap
	topicSettings TopicSettings
	monInterval   bigduration.BigDuration
}

// DefaultTopicSettings sets the default topic settings used if no other is defined at creation time.
func DefaultTopicSettings(settings TopicSettings) Option {
	return func(bl *NetLog) {
		bl.topicSettings = settings
	}
}

// MonitorInterval defines de interval at which the segment monitor in charge of spiting and discarding segments runs.
func MonitorInterval(interval bigduration.BigDuration) Option {
	return func(bl *NetLog) {
		bl.monInterval = interval
	}
}

// NewNetLog creates a new NetLog in a given data folder that must exist and be writable.
func NewNetLog(dataDir string, opts ...Option) (nl *NetLog, err error) {
	d, err := os.Stat(dataDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dataDir, 0755)
		if err != nil {
			log.Printf("error: failed to create data dir: %s", err)
			return nil, ErrInvalidDir
		}
	} else if !d.IsDir() {
		return nil, ErrInvalidDir
	}

	nl = &NetLog{
		topics:  NewTopicAtomicMap(),
		dataDir: dataDir,
	}

	for _, opt := range opts {
		opt(nl)
	}

	err = nl.loadTopics()

	mi := nl.monInterval.Duration()
	if mi == 0 {
		mi = time.Second
	}

	sm := &SegmentMonitor{nl: nl}
	go sm.start(mi)

	return nl, err
}

func (nl *NetLog) loadTopics() (err error) {
	dirfs, err := ioutil.ReadDir(nl.dataDir)
	if err != nil {
		return err
	}

	for _, f := range dirfs {
		if f.IsDir() {
			err = nl.loadTopic(f.Name())
			if err != nil {
				log.Printf("error: failed to load topic %q error: %s", f.Name(), err)
				break
			}
		}
	}

	return err
}

func (nl *NetLog) loadTopic(name string) (err error) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("alert: segment load failed name=%s %s", name, err)
		}
	}()

	if t, _ := nl.Topic(name); t != nil {
		return ErrTopicExists
	}

	topicPath := filepath.Join(nl.dataDir, name)

	bl, err := biglog.Open(topicPath)
	if err != nil {
		return err
	}

	settingsPath := filepath.Join(topicPath, settingsFile)
	f, err := os.OpenFile(settingsPath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	dec := json.NewDecoder(f)
	var settings TopicSettings
	err = dec.Decode(&settings)
	if err != nil {
		return err
	}

	t := newTopic(bl, settings, nl.topicSettings)
	return nl.register(name, t)
}

// CreateTopic creates a new topic with a given name and default settings.
func (nl *NetLog) CreateTopic(name string, settings TopicSettings) (t *Topic, err error) {
	defer func() {
		if err != nil {
			log.Printf("warn: failed to create topic %q: %s", name, err)
		}
	}()

	if t, _ = nl.Topic(name); t != nil {
		return t, ErrTopicExists
	}

	topicPath := filepath.Join(nl.dataDir, name)
	bl, err := biglog.Create(topicPath, 100*1024)
	if err != nil {
		return nil, err
	}

	t = newTopic(bl, settings, nl.topicSettings)
	err = nl.register(name, t)
	if err != nil {
		return nil, err
	}

	settingsPath := filepath.Join(topicPath, settingsFile)
	f, err := os.OpenFile(settingsPath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	enc := json.NewEncoder(f)
	err = enc.Encode(t.settings)
	if err != nil {
		panic(err)
	}

	return t, err
}

// Topic returns an existing topic by name.
func (nl *NetLog) Topic(name string) (*Topic, error) {
	if topic, ok := nl.topics.Get(name); ok {
		return topic, nil
	}

	return nil, ErrTopicNotFound
}

// DeleteTopic deletes an existing topic by name.
func (nl *NetLog) DeleteTopic(name string, force bool) (err error) {
	defer func() {
		if err != nil {
			log.Printf("warn: failed to delete topic %q: %s", name, err)
		}
	}()

	log.Printf("info: deleting topic %q force=%t", name, force)
	t, err := nl.Topic(name)
	if err != nil {
		return err
	}

	// first unregister to prevent usage
	// during the deletion process
	err = nl.unregister(name)
	if err != nil {
		return err
	}

	err = t.bl.Delete(force)
	if err != nil {
		// in case of error register back
		_ = nl.register(name, t)
		return err
	}

	log.Printf("info: deleted topic %q force=%t", name, force)
	return nil
}

// TopicList returns the list of existing topic names.
func (nl *NetLog) TopicList() []string {
	m := nl.topics.GetAll()
	list := make([]string, 0, len(m))
	for name := range m {
		list = append(list, name)
	}

	return list
}

func (nl *NetLog) register(name string, topic *Topic) error {
	if t, _ := nl.Topic(name); t != nil {
		return ErrTopicExists
	}

	nl.topics.Set(name, topic)
	return nil
}

func (nl *NetLog) unregister(name string) error {
	if _, err := nl.Topic(name); err != nil {
		return err
	}

	nl.topics.Delete(name)
	return nil
}
