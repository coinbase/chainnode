package api

import (
	"fmt"
	"strconv"

	"golang.org/x/xerrors"
)

// Sequence is a mono-increasing number associated with the ChainStorage block stream.
// It could have one of the following representations:
// - int64: underlying type
// - decimal string: used by ChainStorage streaming events
// - hex-encoded string with zero padding: used as sort key in storage
type Sequence int64

const (
	InitialSequence Sequence = 0
)

func ParsePaddedHexSequence(s string) (Sequence, error) {
	v, err := strconv.ParseInt(s, 16, 64)
	if err != nil {
		return 0, xerrors.Errorf("failed to parse int64 (%v): %w", s, err)
	}

	return Sequence(v), nil
}

func ParseSequenceNum(s int64) Sequence {
	return Sequence(s)
}

func (s Sequence) AsDecimal() string {
	return strconv.FormatInt(int64(s), 10)
}

func (s Sequence) AsPaddedHex() string {
	return fmt.Sprintf("%016s", strconv.FormatInt(int64(s), 16))
}

func (s Sequence) AsInt64() int64 {
	return int64(s)
}
