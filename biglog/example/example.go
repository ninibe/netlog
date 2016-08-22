package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"log"

	"github.com/ninibe/netlog/biglog"
)

func main() {
	usage := `Usage: go run example.go [step1] [step2] ...`

	if len(os.Args) == 1 {
		println(usage)
		return
	}

	switch os.Args[1] {
	case "step1":
		step1()
	case "step2":
		step2()
	case "step3":
		step3()
	case "step4":
		step4()
	case "step5":
		step5()
	default:
		println(usage)
	}
}

// Creating a writing demo
func step1() {
	// Create a BigLog called demo, with a preallocated index size of 10 entries per segments
	// 10 is for demo purposes, is should be 10k-10M depending on your use cases
	bl, err := biglog.Create("demo", 10)
	panicOn(err)
	defer logClose(bl)

	// Write one entry, could be any blob of bytes
	_, err = bl.Write([]byte("some data"))
	panicOn(err)

	println("inspect your segment:")
	println("xxd demo/00000000000000000000.index")
	println("xxd demo/00000000000000000000.data")
}

// Split segment demo
func step2() {
	// Open an existing BigLog
	bl, err := biglog.Open("demo")
	panicOn(err)
	defer logClose(bl)

	// Write more data
	_, err = bl.Write([]byte("some more data"))
	panicOn(err)

	// Create a new segment
	err = bl.Split()
	panicOn(err)

	// Writes go the the new segment
	_, err = bl.Write([]byte("new segment data"))
	panicOn(err)

	println("inspect your segments:")
	println("ls demo")
	println("xxd demo/00000000000000000002.index")
	println("xxd demo/00000000000000000002.data")
}

// readers demo
func step3() {
	// Open an existing BigLog
	bl, err := biglog.Open("demo")
	panicOn(err)
	defer logClose(bl)

	// Create a new index reader
	ir, _, err := biglog.NewIndexReader(bl, 0)
	panicOn(err)
	defer logClose(ir)

	// Read the entries stored
	entries, err := ir.ReadEntries(10)
	println("Entries:")
	for _, entry := range entries {
		fmt.Printf("%+v\n", entry)
	}

	// Create a new data reader
	r, _, err := biglog.NewReader(bl, 0)
	panicOn(err)
	defer logClose(r)

	data, err := ioutil.ReadAll(r)
	panicOn(err)
	fmt.Printf("Data:\n%s\n", data)
}

// scanner demo
func step4() {
	// Open an existing BigLog
	bl, err := biglog.Open("demo")
	panicOn(err)
	defer logClose(bl)

	// WriteN means the written data contains 5 offsets
	_, err = bl.WriteN([]byte("this is data for 5 offsets"), 5)
	panicOn(err)

	_, err = bl.Write([]byte("another single message"))
	panicOn(err)

	// Create an scanner from the beginning of the log
	sc, err := biglog.NewScanner(bl, 0)
	panicOn(err)
	defer logClose(sc)

	for {
		if !sc.Scan() {
			break
		}
		fmt.Printf("offset = %d n = %d data = %s\n", sc.Offset(), sc.ODelta(), sc.Bytes())
	}
}

// watcher demo
func step5() {
	// Open an existing BigLog
	bl, err := biglog.Open("demo")
	panicOn(err)
	defer logClose(bl)

	wc := biglog.NewWatcher(bl)
	defer logClose(wc)

	go func() {
		for {
			<-wc.Watch()
			println("write detected")
		}
	}()

	for i := 0; i < 5; i++ {
		_, _ = bl.Write([]byte("foo"))
		time.Sleep(time.Second)
	}
}

func panicOn(err error) {
	if err != nil && err != io.EOF {
		panic(err)
	}
}

func logClose(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Printf("error: %s", err)
	}
}
