// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package netlog

import (
	"golang.org/x/net/context"

	"github.com/ninibe/netlog/biglog"
	"github.com/ninibe/netlog/message"
)

const errLimit = 1000

// IntegrityChecker is used to check the integrity of an entire topic.
type IntegrityChecker struct {
	sc *biglog.Scanner
}

// NewIntegrityChecker creates a new integrity checker for a given topic.
func NewIntegrityChecker(t *Topic, from int64) (*IntegrityChecker, error) {
	sc, err := biglog.NewScanner(t.bl, from)
	if err != nil {
		return nil, err
	}

	return &IntegrityChecker{sc: sc}, nil
}

// Check reads all data collecting errors which then returns.
// Is recommended to pass a cancellable context since this operation can be slow.
func (ic *IntegrityChecker) Check(ctx context.Context) (errors []*message.IntegrityError) {
	for {
		if len(errors) >= errLimit {
			return errors
		}

		select {
		case <-ctx.Done():
			return errors
		default:
		}

		m, o, d, err := ic.scan()
		if err == ErrEndOfTopic {
			return errors
		}

		if err != nil {
			errors = append(errors, &message.IntegrityError{
				Offset: o,
				ODelta: d,
				Type:   message.IntegrityUnknownErr,
				Actual: err.Error(),
			})

			continue
		}

		iErr := message.CheckMessageIntegrity(m, d)
		if iErr != nil {
			iErr.Offset = o
			errors = append(errors, iErr)
		}
	}
}

func (ic *IntegrityChecker) scan() (message.Message, int64, int, error) {
	ok := ic.sc.Scan()
	if ok {
		return message.Message(ic.sc.Bytes()),
			ic.sc.Offset(),
			ic.sc.ODelta(),
			nil
	}

	if ic.sc.Err() != nil {
		return nil, -1, -1, ic.sc.Err()
	}

	return nil, -1, -1, ErrEndOfTopic
}

// Close releases the underlying resources.
func (ic *IntegrityChecker) Close() error {
	return ic.sc.Close()
}
