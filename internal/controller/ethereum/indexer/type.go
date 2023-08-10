package indexer

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"golang.org/x/xerrors"
)

type (
	EthereumQuantity  uint64
	EthereumHexString string
)

func (v EthereumQuantity) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf(`"%s"`, hexutil.EncodeUint64(uint64(v)))
	return []byte(s), nil
}

func (v *EthereumQuantity) UnmarshalJSON(input []byte) error {
	if len(input) > 0 && input[0] != '"' {
		var i uint64
		if err := json.Unmarshal(input, &i); err != nil {
			return xerrors.Errorf("failed to unmarshal EthereumQuantity into uint64: %w", err)
		}

		*v = EthereumQuantity(i)
		return nil
	}

	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal EthereumQuantity into string: %w", err)
	}

	if s == "" {
		*v = 0
		return nil
	}

	i, err := hexutil.DecodeUint64(s)
	if err != nil {
		return xerrors.Errorf("failed to decode EthereumQuantity %v: %w", s, err)
	}

	*v = EthereumQuantity(i)
	return nil
}

func (v EthereumQuantity) Value() uint64 {
	return uint64(v)
}

func (v *EthereumQuantity) AsTime() time.Time {
	if v.Value() == 0 {
		// if v is not set, return an empty time
		return time.Time{}.UTC()
	}

	return time.Unix(int64(v.Value()), 0).UTC()
}

func (v EthereumHexString) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf(`"%s"`, v)
	return []byte(s), nil
}

func (v *EthereumHexString) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return xerrors.Errorf("failed to unmarshal EthereumHexString: %w", err)
	}
	s = strings.ToLower(s)

	*v = EthereumHexString(s)
	return nil
}

func (v EthereumHexString) Value() string {
	return string(v)
}
