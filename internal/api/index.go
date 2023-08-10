package api

import (
	"fmt"
	"strconv"

	"golang.org/x/xerrors"
)

// Index is an index number in some slices like transactions and logs
// It could have one of the following representations:
// - uint64: underlying type
// - hex-encoded string with zero padding: used as sort key in storage
type Index uint64

func ParsePaddedHexIndex(s string) (Index, error) {
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, xerrors.Errorf("failed to parse uint64 (%v): %w", s, err)
	}

	return Index(v), nil
}

func (i Index) AsPaddedHex() string {
	return fmt.Sprintf("%016s", strconv.FormatUint(uint64(i), 16))
}
