package collection

import (
	"time"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/utils/timeutil"
)

type (
	Item interface {
		// GetPartitionKey gets the partition key of the entry
		GetPartitionKey() (string, error)
		// GetSortKey gets the sort key of the entry
		GetSortKey() (string, error)
		// GetTag gets the tag of the entry
		GetTag() (uint32, error)
		// GetObjectKey gets the object key of the entry
		GetObjectKey() (string, error)
		// GetData gets the value of the data column
		GetData() ([]byte, error)
		// GetUpdatedAt gets the UpdatedAt timestamp
		GetUpdatedAt() (string, error)

		// MakeObjectKey makes the object key to the blob storage which data gets uploaded to
		MakeObjectKey() (string, error)

		// SetObjectKey sets the object key of the entry
		SetObjectKey(string) error
		// SetData sets the data of the entry
		SetData(data []byte) error

		// AsAPI converts interface to an api object
		AsAPI(value interface{}) error

		// ParseTimestamp parses timestamp string in ISO8601 format into time.Time
		ParseTimestamp(value string, optional bool) (time.Time, error)
	}

	BaseItem struct {
		Item

		PartitionKey string `dynamodbav:"pk"`
		SortKey      string `dynamodbav:"sk"`
		Tag          uint32 `dynamodbav:"tag"`
		Data         []byte `dynamodbav:"data"`
		ObjectKey    string `dynamodbav:"object_key"`
		UpdatedAt    string `dynamodbav:"updated_at"`
	}
)

func NewBaseItem(partitionKey, sortKey string, tag uint32) *BaseItem {
	return &BaseItem{
		PartitionKey: partitionKey,
		SortKey:      sortKey,
		Tag:          tag,
		UpdatedAt:    timeutil.TimeToISO8601(time.Now()),
	}
}

func (i *BaseItem) WithData(data []byte) *BaseItem {
	i.Data = data
	return i
}

func (i *BaseItem) WithObjectKey(objectKey string) *BaseItem {
	i.ObjectKey = objectKey
	return i
}

func (i *BaseItem) GetPartitionKey() (string, error) {
	if i.PartitionKey == "" {
		return "", xerrors.Errorf("partition key should not be empty")
	}

	return i.PartitionKey, nil
}

func (i *BaseItem) GetSortKey() (string, error) {
	if i.SortKey == "" {
		return "", xerrors.Errorf("sort key should not be empty")
	}

	return i.SortKey, nil
}

func (i *BaseItem) GetTag() (uint32, error) {
	if i.Tag == 0 {
		return 0, xerrors.Errorf("tag should not be empty")
	}

	return i.Tag, nil
}

func (i *BaseItem) GetData() ([]byte, error) {
	return i.Data, nil
}

func (i *BaseItem) GetObjectKey() (string, error) {
	return i.ObjectKey, nil
}

func (i *BaseItem) GetUpdatedAt() (string, error) {
	return i.UpdatedAt, nil
}

func (i *BaseItem) SetData(data []byte) error {
	i.Data = data
	return nil
}

func (i *BaseItem) SetObjectKey(objectKey string) error {
	i.ObjectKey = objectKey
	return nil
}

func (i *BaseItem) ParseTimestamp(value string, optional bool) (time.Time, error) {
	var t time.Time
	if optional {
		if value == "" {
			return t, nil
		}
	}

	return timeutil.ParseISO8601(value)
}
