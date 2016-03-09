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

[![Build Status](https://travis-ci.org/ninibe/netlog.svg?branch=master)](https://travis-ci.org/ninibe/netlog)&nbsp;[![godoc reference](https://godoc.org/github.com/ninibe/netlog?status.png)](https://godoc.org/github.com/ninibe/netlog)