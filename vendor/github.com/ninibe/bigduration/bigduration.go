package bigduration

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	Day   = 24 * time.Hour
	Month = 30 * Day
	Year  = 365 * Day

	dayUnit   = "day"
	monthUnit = "month"
	yearUnit  = "year"
)

var (
	units = []string{yearUnit, monthUnit, dayUnit}
	durts = []time.Duration{Year, Month, Day}
)

// BigDuration represents a duration in years (365 days), months (30 days), weeks and days
// alongside stadard time.Duration that can be parsed from hours, minutes and seconds.
type BigDuration struct {
	Years  int
	Months int
	Days   int
	Nanos  time.Duration
}

// Duration returns equivalent time.Duration
func (bd *BigDuration) Duration() time.Duration {
	return time.Duration(bd.Years)*Year +
		time.Duration(bd.Months)*Month +
		time.Duration(bd.Days)*Day +
		bd.Nanos
}

// String returns a string representing the duration in the form "2month23days2h3m0.5s".
// Leading zero units are omitted.
func (bd *BigDuration) String() string {

	final := ""

	if bd.Years > 0 {
		final = fmt.Sprintf("%d%s", bd.Years, yearUnit)
	}

	if bd.Months > 0 {
		final = fmt.Sprintf("%s%d%s", final, bd.Months, monthUnit)
	}

	if bd.Days > 0 {
		final = fmt.Sprintf("%s%d%s", final, bd.Days, dayUnit)
	}

	if bd.Nanos > 0 {
		final = fmt.Sprintf("%s%s", final, bd.Nanos)
	}

	return final
}

// Compact returns a string representation compacting smaller units into bigger ones when possible
func (bd *BigDuration) Compact() string {
	total := bd.Duration()
	final := ""

	for k, dur := range durts {
		count := 0
		for {
			if total < dur {
				break
			}

			total -= dur
			count++
		}

		if count > 0 {
			final = fmt.Sprintf("%s%d%s", final, count, units[k])
		}
	}

	if total > 0 {
		final = fmt.Sprintf("%s%s", final, total)
	}

	return final
}

// ParseBigDuration parses a BigDuration string simply splitting by keyword
// and then using time.ParseDuration for smaller units
func ParseBigDuration(s string) (bd BigDuration, err error) {
	counts := make([]int, len(units))
	for k, unit := range units {
		chunks := strings.Split(s, unit)
		if len(chunks) == 2 {
			counts[k], err = strconv.Atoi(chunks[0])
			if err != nil {
				return bd, err
			}
			s = chunks[1]
		}
	}

	if s != "" {
		bd.Nanos, err = time.ParseDuration(s)
		if err != nil {
			return bd, err
		}
	}

	bd.Years = counts[0]
	bd.Months = counts[1]
	bd.Days = counts[2]

	return bd, err
}

// From takes a time and returns the time adding the big duration in a calendar sensitive way
// months are no longer 30 days but calendar months instead, leap years also accounted for.
func (bd *BigDuration) From(u time.Time) time.Time {
	return u.AddDate(bd.Years, bd.Months, bd.Days).Add(bd.Nanos)
}

// Until takes a time and returns the time subtracting the big duration in a calendar sensitive way
// months are no longer 30 days but calendar months instead, leap years also accounted for.
func (bd *BigDuration) Until(u time.Time) time.Time {
	return u.AddDate(-1*bd.Years, -1*bd.Months, -1*bd.Days).Add(-1 * bd.Nanos)
}

// Add returns the sum of both big durations
func (bd *BigDuration) Add(u BigDuration) BigDuration {
	return BigDuration{
		Years:  bd.Years + u.Years,
		Months: bd.Months + u.Months,
		Days:   bd.Days + u.Days,
		Nanos:  bd.Nanos + u.Nanos,
	}
}

// MarshalJSON implements the json.Marshaler interface.
func (bd BigDuration) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"%s"`, bd.String())), nil
}

// UnmarshalJSON implements the json.Unmarshaler interface.
func (bd *BigDuration) UnmarshalJSON(data []byte) (err error) {
	*bd, err = ParseBigDuration(string(data[1 : len(data)-1]))
	return err
}
