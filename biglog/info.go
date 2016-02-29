// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package biglog

import "time"

// Info holds all BigLog meta data
type Info struct {
	Name         string     `json:"name"`
	Path         string     `json:"path"`
	DiskSize     int64      `json:"disk_size"`
	FirstOffset  int64      `json:"first_offset"`
	LatestOffset int64      `json:"latest_offset"`
	Segments     []*SegInfo `json:"segments"`
	ModTime      time.Time  `json:"mod_time"`
}

// Info returns an Info struct with all information about the BigLog.
func (bl *BigLog) Info() (*Info, error) {

	inf := &Info{
		Name:         bl.name,
		Path:         bl.dirPath,
		FirstOffset:  bl.Oldest(),
		LatestOffset: bl.Latest(),
	}

	segs := bl.segments()
	for k := range segs {
		si, err := segs[k].Info()
		if err != nil {
			return nil, err
		}
		inf.Segments = append(inf.Segments, si)
		inf.DiskSize += si.DiskSize
		inf.ModTime = si.ModTime
	}

	return inf, nil
}
