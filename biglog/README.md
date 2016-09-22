# BigLog

BigLog is a high level abstraction on top of os.File designed to store large amounts of data. BigLog is NetLog's core component, which can be embedded in any application.

In a log-based queue, logs must be append-only. But most people eventually need to delete data, so instead of a single file we have several "segments". Every segment is just a data file with blob of bytes written to it and a companion index file. Indexes are preallocated in a fixed size every time a segment is created and memory-mapped. Each entry in the index has the format:

### Index entry format

```
+---------------------------------------------------------------+
| relative offset   |   unix timestamp   |   data file offset   |
+---------------------------------------------------------------+
| (4bytes)[uint32]  |  (4bytes)[uint32]  |   (8bytes)[int64]    |
+---------------------------------------------------------------+
```

Every segment has a base offset, and the index stores the relative offset to that base, with the first offset being always 1. The index can be sparse. The last element of the index is always the NEXT offset to be written, whose timestamp is not set.

### Index example

```
+-----------------------------------+
|    1   |  1456514390   |      0   |  <- first RO, offset 0 in data file
+-----------------------------------+
|    2   |  1456514391   |     32   |  <- RO 2 starts 32 bytes later
+-----------------------------------+
|    4   |  1456514392   |     96   |  <- RO 4 starts 64 bytes later
+-----------------------------------+     RO 3 is embedded somewhere between position 32 and 96
|   11   |           0   |    320   |  <- Next available offset is RO11 which goes at position 320
+-----------------------------------+     (size of the data file)
```

The segment with the highest base offset is the "hot" segment, the only one which gets writes under the hood via Write() [io.Writer interface] for a single offset or WriteN() for N offsets. You can create a new hot segment calling Split(), and discard the oldest one calling Trim().

There are 2 reading primitives, a Reader [io.Reader] which reads over the data files returning byte blobs, and an Index Reader which reads index files returning entries. Both are initialized (multiple instances allowed) and operate separately.

```
                     +------------------+
                  -> |   index reader   | ->
                     +------------------+
+-----------------------------------+ +-----------------------------------+
|           index file 0            | |           index file 1            |
+-----------------------------------+ +-----------------------------------+
+-----------------------------------+ +-----------------------------------+
|            data file 0            | |            data file 1            |
+-----------------------------------+ +-----------------------------------+
                             +------------------+
                          -> |    data reader   | ->
                             +------------------+
```

Readers will transparently jump through segments until their buffer is full or EOF is reached, they can be instantiated to start at any give offset with a specific entry in the index, if an embedded offset is requested the reader will start in the previous known offset position.

Based on these 2 readers, BigLog provides another 2 higher abstractions, Scanner and Streamer. [See the godocs](https://godoc.org/github.com/ninibe/netlog/biglog).
