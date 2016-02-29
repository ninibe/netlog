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
)

func tempNetLog() *NetLog {
	rand.Seed(int64(time.Now().Nanosecond()))
	dataDir := filepath.Join(os.TempDir(), fmt.Sprintf("netlogtest-%d", rand.Int63()))
	err := os.Mkdir(dataDir, 0777)
	panicOn(err)
	s, err := NewNetLog(dataDir)
	panicOn(err)

	return s
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
