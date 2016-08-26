// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package main

import (
	"flag"
	"log"
	"net/http"

	"comail.io/go/colog"
	"golang.org/x/net/http2"

	"github.com/ninibe/bigduration"
	"github.com/ninibe/netlog"
	"github.com/ninibe/netlog/message"
	"github.com/ninibe/netlog/transport"
)

var (
	debug         = flag.Bool("debug", false, "Start on debug mode")
	listen        = flag.String("listen", ":7200", "Listen address")
	dataDir       = flag.String("dir", "./data", "Data folder")
	logLevel      = flag.String("loglevel", "info", "Logging level")
	monInterval   = flag.String("monitor_interval", "10s", "Interval for segment size and age checks")
	segAge        = flag.String("segment_age", "30day", "Time since the last write in a segment until it gets discarded")
	segSize       = flag.Int64("segment_size", 1024*1024*1024, "Maximum topic segment size in bytes")
	batchNum      = flag.Int("batch_num_messages", 100, "Default maximum number of messages to be batched")
	batchInterval = flag.String("batch_interval", "200ms", "Default interval at which batched messages are flushed to disk.")
	compression   = flag.Int("compression", 1, "Default compression for batches: 1 = gzip, 2 = snappy, 3 = none")
)

func main() {
	flag.Parse()
	colog.Register()

	ll, err := colog.ParseLevel(*logLevel)
	fatalOn(err)
	colog.SetMinLevel(ll)

	if *debug {
		colog.SetFlags(log.LstdFlags | log.Lshortfile)
		colog.SetMinLevel(colog.LTrace)
	}

	var server http.Server
	server.Addr = *listen
	err = http2.ConfigureServer(&server, nil)
	fatalOn(err)

	segAge, err := bigduration.ParseBigDuration(*segAge)
	fatalOn(err)
	mIterval, err := bigduration.ParseBigDuration(*monInterval)
	fatalOn(err)

	bInterval, err := bigduration.ParseBigDuration(*batchInterval)
	fatalOn(err)

	topSettings := netlog.TopicSettings{
		SegAge:           segAge,
		SegSize:          *segSize,
		BatchNumMessages: *batchNum,
		BatchInterval:    bInterval,
		CompressionType:  message.CompressionType(*compression),
	}

	nl, err := netlog.NewNetLog(*dataDir,
		netlog.DefaultTopicSettings(topSettings),
		netlog.MonitorInterval(mIterval))
	fatalOn(err)

	http.Handle("/", transport.NewHTTPTransport(nl))
	log.Printf("info: listening on %q", server.Addr)
	log.Printf("info: data dir on %q", *dataDir)
	log.Fatalf("alert: %s\n", server.ListenAndServe())
}

func fatalOn(err error) {
	if err != nil {
		log.Fatalf("alert: %s\n", err)
	}
}
