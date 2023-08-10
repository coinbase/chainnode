package timeutil

import (
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestTimeToISO8601(t *testing.T) {
	require := testutil.Require(t)
	var nilTime time.Time
	tests := []struct {
		name     string
		date     time.Time
		expected string
	}{
		{
			name:     "happy case",
			date:     testutil.MustTime("2020-11-24T16:07:21Z"),
			expected: "2020-11-24T16:07:21Z",
		},
		{
			name:     "zero time",
			date:     time.Time{},
			expected: "",
		},
		{
			name:     "zero time",
			date:     nilTime,
			expected: "",
		},
	}

	for _, test := range tests {
		require.Equal(test.expected, TimeToISO8601(test.date))
	}
}

func TestParseISO8601(t *testing.T) {
	require := testutil.Require(t)
	tests := []struct {
		name     string
		value    string
		expected time.Time
		err      bool
	}{
		{
			name:     "happy case",
			value:    "2020-11-24T16:07:21Z",
			expected: testutil.MustTime("2020-11-24T16:07:21Z"),
			err:      false,
		},
		{
			name:     "empty",
			value:    "",
			expected: time.Time{},
			err:      true,
		},
	}

	for _, test := range tests {
		t, err := ParseISO8601(test.value)
		if test.err {
			require.Error(err)
		} else {
			require.NoError(err)
			require.Equal(test.expected, t)
		}
	}
}

func TestTimestampToTime(t *testing.T) {
	require := testutil.Require(t)

	ts := &timestamp.Timestamp{Seconds: time.Now().Unix() - 1}
	value := TimestampToTime(ts)
	require.NotZero(value)

	ts = &timestamp.Timestamp{}
	value = TimestampToTime(ts)
	require.Zero(value)

	var nilTs *timestamp.Timestamp
	value = TimestampToTime(nilTs)
	require.Zero(value)
}

func TestSinceTimestamp(t *testing.T) {
	require := testutil.Require(t)
	duration := SinceTimestamp(nil)
	require.Zero(duration)
	duration = SinceTimestamp(&timestamp.Timestamp{Seconds: 0})
	require.Zero(duration)

	ts := &timestamp.Timestamp{Seconds: time.Now().Unix() - 1}
	duration = SinceTimestamp(ts)
	require.NotZero(duration)
	require.Less(duration, time.Second*2)
}
