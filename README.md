[![Build Status](https://travis-ci.org/ninibe/netlog.svg?branch=master)](https://travis-ci.org/ninibe/netlog)&nbsp;[![godoc reference](https://godoc.org/github.com/ninibe/netlog?status.png)](https://godoc.org/github.com/ninibe/netlog)

# NetLog
A lightweight, HTTP-centric, log-based (Kafka-style) message queue.

## Work-in-progress!
This is still experimental software potentially full of bugs.
To peek at the internals start with [BigLog](https://github.com/ninibe/netlog/tree/master/biglog).

### Roadmap

- [x] low-level log management
- [x] HTTP transport
- [x] scanner based pub/sub
- [x] custom data retention policy
- [x] persistent scanners
- [x] batching
- [x] compression
- [ ] good test coverage
- [ ] proper documentation
- [ ] streaming based pub/sub
- [ ] async replication
- [ ] kinesis-compatible transport
- [ ] gRPC transport

### Non-goals
* Match Kafka's performance.
* Distributed system.

### Getting started

While posting and fetching single messages is very inefficient, it's the simplest way to get started using nothing but curl commands.

```bash
# compile server
go get github.com/ninibe/netlog/cmd/netlog

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

### One-line-ish pub/sub
```bash
# create new topic
curl -XPOST localhost:7200/pubsubdemo

# get scanner ID with jq
export SCANNER=$(curl -s -XPOST "localhost:7200/pubsubdemo/scanner?from=0&persist=true" | jq -r .id)

# subscribe to the topic
while true; do; curl "localhost:7200/pubsubdemo/scan?id=$SCANNER&wait=1h" && echo; done

# IN ANOTHER WINDOW

# publish on the topic
while true; do; read data; curl localhost:7200/pubsubdemo/payload --data-binary $data; done
# write something and hit enter

```
