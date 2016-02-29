// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"time"
)

func setupData(size int) *BigLog {
	bl := tempBigLog()
	data := randData(size)
	// write some data with increasing number of offsets per entry
	// offsets 0, 1, 3, 6, 10, 15, 21, 28, 36 - 45
	for i := 1; i < 10; i++ {
		if i == 5 {
			bl.Split()
		}

		_, err := bl.WriteN(data, i)
		panicOn(err)
	}

	return bl
}

func tempBigLog() *BigLog {
	rand.Seed(int64(time.Now().Nanosecond()))
	datadir := filepath.Join(os.TempDir(), fmt.Sprintf("netlogtest-%d", rand.Int63()))
	bl, err := Create(datadir, 500)
	panicOn(err)

	return bl
}

var dictionary = "0123456789abcdefghijklmnopqrstuvwxyz"

func randStr(size int) []byte {
	var bytes = randData(size)
	for k, v := range bytes {
		bytes[k] = dictionary[v%byte(len(dictionary))]
	}

	return bytes
}

func randData(size int) []byte {
	var bytes = make([]byte, size)
	crand.Read(bytes)
	return bytes
}

func randDataSet(entries, size int) [][]byte {
	set := make([][]byte, entries)
	for k := range set {
		set[k] = randStr(size + int(rand.Int31n(int32(size))))
	}

	return set
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}
