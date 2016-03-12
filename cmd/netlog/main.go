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
	compression   = flag.Int("compression", 1, "Default compression for batches: 0 = none, 1 = gzip, 2 = snappy")
)

func main() {
	flag.Parse()
	colog.Register()

	ll, err := colog.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("alert: %s\n", err)
	}
	colog.SetMinLevel(ll)

	if *debug {
		colog.SetFlags(log.LstdFlags | log.Lshortfile)
		colog.SetMinLevel(colog.LTrace)
	}

	var server http.Server
	server.Addr = *listen
	http2.ConfigureServer(&server, nil)

	segAge, err := bigduration.ParseBigDuration(*segAge)
	if err != nil {
		log.Fatalf("alert: %s\n", err)
	}

	mIterval, err := bigduration.ParseBigDuration(*monInterval)
	if err != nil {
		log.Fatalf("alert: %s\n", err)
	}

	bInterval, err := bigduration.ParseBigDuration(*batchInterval)
	if err != nil {
		log.Fatalf("alert: %s\n", err)
	}

	topSettings := netlog.TopicSettings{
		SegAge:           segAge,
		SegSize:          *segSize,
		BatchNumMessages: *batchNum,
		BatchInterval:    bInterval,
		CompressionType:  netlog.CompressionType(*compression),
	}

	nl, err := netlog.NewNetLog(*dataDir,
		netlog.DefaultTopicSettings(topSettings),
		netlog.MonitorInterval(mIterval))
	if err != nil {
		log.Fatalf("alert: %s\n", err)
	}

	http.Handle("/", transport.NewHTTPTransport(nl))
	log.Printf("info: listening on %q", server.Addr)
	log.Printf("info: data dir on %q", *dataDir)
	log.Fatalf("alert: %s\n", server.ListenAndServe())
	//	log.Fatal(server.ListenAndServeTLS(GenCertificate()))
}
