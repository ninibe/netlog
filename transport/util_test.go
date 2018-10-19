// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package transport

import (
	crand "crypto/rand"
	"fmt"
	"math/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"comail.io/go/colog"
	"github.com/ninibe/bigduration"
	"github.com/ninibe/netlog"
)

func init() {
	colog.Register()
	colog.SetMinLevel(colog.LError)
}

func runTestHTTPServer() *httptest.Server {

	rand.Seed(int64(time.Now().Nanosecond()))
	datadir := filepath.Join(os.TempDir(), fmt.Sprintf("netlogtest-%d", rand.Int63()))
	panicOn(os.Mkdir(datadir, 0755))

	bd, err := bigduration.ParseBigDuration("1day")
	panicOn(err)
	nl, err := netlog.NewNetLog(datadir, netlog.MonitorInterval(bd))
	panicOn(err)

	return httptest.NewServer(NewHTTPTransport(nl))
}

func randData(size int) []byte {
	var bytes = make([]byte, size)
	_, _ = crand.Read(bytes)
	return bytes
}

func randDataSet(entries, size int) [][]byte {
	set := make([][]byte, entries)
	for k := range set {
		set[k] = randData(size)
	}

	return set
}

func panicOn(err error) {
	if err != nil {
		panic(err)
	}
}
