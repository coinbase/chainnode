package timeutil

import (
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
)

func TimeToISO8601(date time.Time) string {
	if date.IsZero() {
		return ""
	}

	return date.UTC().Format(time.RFC3339Nano)
}

func ParseISO8601(value string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, err
	}
	return t, nil
}

func TimestampToTime(timestamp *timestamp.Timestamp) time.Time {
	var t time.Time
	if timestamp.GetSeconds() > 0 || timestamp.GetNanos() > 0 {
		if t := timestamp.AsTime(); !t.IsZero() {
			return t
		}
	}
	return t
}

func SinceTimestamp(timestamp *timestamp.Timestamp) time.Duration {
	var res time.Duration
	if timestamp.GetSeconds() > 0 || timestamp.GetNanos() > 0 {
		if t := timestamp.AsTime(); !t.IsZero() {
			res = time.Since(t)
		}
	}

	return res
}
