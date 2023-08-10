package testutil

import (
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
)

func MustTime(value string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return t
}

func ToTimestamp(seconds int64) *timestamp.Timestamp {
	if seconds == 0 {
		return nil
	}

	return &timestamp.Timestamp{
		Seconds: seconds,
	}
}
