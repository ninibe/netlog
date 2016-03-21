[![Build Status](https://travis-ci.org/ninibe/netlog.svg?branch=master)](https://travis-ci.org/ninibe/netlog)&nbsp;[![godoc reference](https://godoc.org/github.com/ninibe/netlog?status.png)](https://godoc.org/github.com/ninibe/netlog)

# NetLog
A lightweight, HTTP-centric, log-based (Kafka-style) message queue.

### Work-in-progress!
To peek at the internals start with [BigLog](https://github.com/ninibe/netlog/tree/master/biglog).

### Initial non-goals
* Match Kafka's performance.
* Distributed system.

### Goals
* Easy to use, curl-friendly, HTTP interface.
* O(1) for read/write operations.
* Master-slave asynchronous replication.

### Getting started

While posting and fetching single messages is very inefficient, it's the simplest way to get started using nothing but curl commands.

```bash
# compile server
go install github.com/ninibe/netlog/cmd/netlog

# run server
bin/netlog

# create new topic
curl -XPOST localhost:7200/demo

# post messages
curl -XPOST localhost:7200/demo/payload --data-binary "message number one"
curl -XPOST localhost:7200/demo/payload --data-binary "message number two"
curl -XPOST localhost:7200/demo/payload --data-binary "message number three"

# check topic info
curl localhost:7200/demo

# create scanner
curl -XPOST "localhost:7200/demo/scanner?from=0"

export SC="...UUID RETURNED..."

# start scanning...
curl -XGET "localhost:7200/demo/scan?id=$SC"
x times ...

# wait 5 seconds for new messages
curl -XGET "localhost:7200/demo/scan?id=$SC&wait=5s"

# wait 5 minutes
curl -XGET "localhost:7200/demo/scan?id=$SC&wait=5m"

# post more messages in another window
curl -XPOST localhost:7200/demo/payload --data-binary "message number four"
curl -XPOST localhost:7200/demo/payload --data-binary "message number five"

# new scanner since 1 minute ago
curl -XPOST "localhost:7200/demo/scanner?from=1m"

```