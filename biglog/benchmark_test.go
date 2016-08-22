// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

var data = []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Etiam volutpat ante in rhoncus commodo.
Morbi id ipsum rutrum, elementum tortor ac, pellentesque libero. Vestibulum tincidunt porta orci.
Vestibulum mattis, augue non mattis malesuada, lectus justo iaculis ipsum, eget euismod sapien nunc vitae nisi.
Nullam eu mattis dui. Etiam faucibus egestas erat sit amet semper. Praesent tincidunt mattis blandit.
Ut sed magna eget lacus ullamcorper semper sed quis felis. Aenean id consequat orci. In hac habitasse platea dictumst.
Nam tincidunt ipsum vitae egestas sollicitudin.`)

func BenchmarkFileWrite(b *testing.B) {
	tmpfile := filepath.Join(os.TempDir(), "writetest")
	f, _ := os.Create(tmpfile)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for i := 0; i <= b.N; i++ {
		_, _ = f.Write(data)
	}
	_ = f.Close()
	_ = os.Remove(tmpfile)
}

func BenchmarkFileWriteSync(b *testing.B) {
	tmpfile := filepath.Join(os.TempDir(), "writetest")
	f, _ := os.Create(tmpfile)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	for i := 0; i <= b.N; i++ {
		_, _ = f.Write(data)
		_ = f.Sync()
	}
	_ = f.Close()
	_ = os.Remove(tmpfile)
}

func BenchmarkSegWrite(b *testing.B) {
	b.StopTimer()

	seg, _ := createSegment(os.TempDir(), 10*1024*1024, rand.Int63())
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		_, _ = seg.WriteN(data, 1)
	}
	b.StopTimer()
	_ = seg.Delete(true)
}

func BenchmarkSegWriteSync(b *testing.B) {
	b.StopTimer()
	seg, _ := createSegment(os.TempDir(), 10*1024*1024, rand.Int63())
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		_, _ = seg.WriteN(data, 1)
		_ = seg.Sync()
	}
	b.StopTimer()
	_ = seg.Delete(true)
}

func BenchmarkBiglogWrite(b *testing.B) {
	b.StopTimer()

	bl, _ := Create(filepath.Join(os.TempDir(), fmt.Sprintf("biglogtest-%d", rand.Int63())), 10*1024*1024)
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		_, _ = bl.Write(data)
	}
	b.StopTimer()
	_ = bl.Delete(true)
}

func BenchmarkBiglogWrite4Kbuf(b *testing.B) {
	b.StopTimer()
	bl, _ := Create(filepath.Join(os.TempDir(), fmt.Sprintf("biglogtest-%d", rand.Int63())), 10*1024*1024)
	bl.SetOpts(BufioWriter(4 * 1024))
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		_, _ = bl.Write(data)
	}
	b.StopTimer()
	_ = bl.Delete(true)
}

func BenchmarkBiglogWrite64Kbuf(b *testing.B) {
	b.StopTimer()
	bl, _ := Create(filepath.Join(os.TempDir(), fmt.Sprintf("biglogtest-%d", rand.Int63())), 10*1024*1024)
	bl.SetOpts(BufioWriter(64 * 1024))
	b.ReportAllocs()
	b.SetBytes(int64(len(data)))
	b.StartTimer()
	for i := 0; i <= b.N; i++ {
		_, _ = bl.Write(data)
	}
	b.StopTimer()
	_ = bl.Delete(true)
}
