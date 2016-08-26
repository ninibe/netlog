// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package message

import (
	crand "crypto/rand"
	"math/rand"

	"comail.io/go/colog"
)

func init() {
	colog.Register()
	colog.SetMinLevel(colog.LError)
}

func randMessageSet() []Message {
	// random number of payloads with random bytes
	data := make([][]byte, rand.Intn(90)+10)
	for k := range data {
		data[k] = randData(rand.Intn(90) + 10)
	}

	msgs := make([]Message, len(data))
	for k := range data {
		msgs[k] = FromPayload(data[k])
	}

	return msgs
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
