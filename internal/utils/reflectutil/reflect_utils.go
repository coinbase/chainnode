package reflectutil

import (
	"reflect"

	"golang.org/x/xerrors"
)

// Populate populates the value from `from` to the `to` pointer
func Populate(to interface{}, from interface{}) error {
	toValue := reflect.ValueOf(to)
	if toValue.Type().Kind() != reflect.Ptr {
		return xerrors.New("to is not a pointer")
	}

	fromValue := reflect.ValueOf(from)
	if fromValue.Type().Kind() != reflect.Ptr {
		return xerrors.New("from is not a pointer")
	}

	toValue.Elem().Set(fromValue.Elem())
	return nil
}
