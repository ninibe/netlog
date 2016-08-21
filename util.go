package netlog

import (
	"io"
	"log"
)

// logClose calls Close on the subject and logs the error if any
// this is handy to call Close on defer
func logClose(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Printf("error: %s", err)
	}
}
