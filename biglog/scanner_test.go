// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
)

func TestScanner(t *testing.T) {

	var readers = 200
	bl := tempBigLog()
	data := randDataSet(2398, 1024)

	for k := range data {
		n, err := bl.Write(data[k])
		if err != nil {
			t.Error(err)
		}
		if n != len(data[k]) {
			t.Errorf("Wrote %d bytes. Expected %d", n, len(data[k]))
		}
	}

	var wg sync.WaitGroup

	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func(data [][]byte, i int) {
			var offset = int64(i) // each reader starts one offset later
			sc, err := NewScanner(bl, offset)
			if err != nil {
				t.Error(err)
			}

			var prevActual []byte
			var prevOffset int64
			var prevDelta int

			for k := i; k < len(data); k++ {

				if !sc.Scan() {
					t.Errorf("Scanner finished before end of loop: i=%d k=%d\n", i, k)
					continue
				}

				if sc.Offset() != offset {
					t.Errorf("Bad offset Actual: %d\n Expected: %d\n Prev: %d \n",
						sc.Offset(),
						offset,
						prevOffset,
					)

					os.Exit(1)
				}

				if !bytes.Equal(sc.Bytes(), data[k]) {
					fmt.Printf("%d => %s \n \n", k, data[k])
					defer wg.Done()

					t.Errorf("fatal payload read i=%d k=%d offset=%d delta=%d error:\n"+
						" Actual: % x\n Expected: % x\n Prev: % x \n  prevOffset=%d prevDelta=%d \n",
						i,

						k,
						sc.Offset(),
						sc.ODelta(),
						sc.Bytes(),
						data[k],
						prevActual,
						prevOffset,
						prevDelta,
					)

					os.Exit(1)
				}

				prevActual = sc.Bytes()
				prevOffset = sc.Offset()
				prevDelta = sc.ODelta()
				offset++
			}

			wg.Done()
		}(data, i)
	}

	wg.Wait()

	err := bl.Delete(false)
	if err != ErrBusy {
		t.Error(err)
	}

	err = bl.Delete(true)
	if err != nil {
		t.Error(err)
	}
}
