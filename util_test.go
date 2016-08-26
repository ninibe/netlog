// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"comail.io/go/colog"

	"github.com/ninibe/bigduration"
	"github.com/ninibe/netlog/message"
)

func init() {
	colog.Register()
	colog.SetMinLevel(colog.LError)
}

func tempNetLog() *NetLog {
	rand.Seed(int64(time.Now().Nanosecond()))

	logName := fmt.Sprintf("netlogtest-%d", rand.Int63())
	dataDir := filepath.Join(os.TempDir(), logName)
	err := os.Mkdir(dataDir, 0777)
	panicOn(err)

	longTime, err := bigduration.ParseBigDuration("1day")
	panicOn(err)

	s, err := NewNetLog(dataDir, MonitorInterval(longTime))
	panicOn(err)

	return s
}

func randMessageSet() []message.Message {
	// random number of payloads with random bytes
	data := make([][]byte, rand.Intn(90)+10)
	for k := range data {
		data[k] = randData(rand.Intn(90) + 10)
	}

	messages := make([]message.Message, len(data))
	for k := range data {
		messages[k] = message.FromPayload(data[k])
	}

	return messages
}

var dictionary = "0123456789abcdefghijklmnopqrstuvwxyz"

func randStr(size int) string {
	var bytes = randData(size)
	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}

	return string(bytes)
}

func randData(size int) []byte {
	var bytes = make([]byte, size)
	_, _ = crand.Read(bytes)
	return bytes
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}
