package jsonutil

import (
	"encoding/json"
	"reflect"

	"golang.org/x/xerrors"
)

// FormatJSON pretty-formats the object.
func FormatJSON(input interface{}) (string, error) {
	output, err := json.MarshalIndent(input, "", "  ")
	if err != nil {
		return "", xerrors.Errorf("failed to marshal result: %w", err)
	}

	return string(output), nil
}

// FilterNulls recursively remove null fields from the object.
func FilterNulls(v interface{}) {
	filterNulls(reflect.ValueOf(v))
}

func filterNulls(v reflect.Value) {
	// Indirect through pointers and interfaces.
	for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.Map:
		for _, key := range v.MapKeys() {
			val := v.MapIndex(key)
			if val.IsNil() {
				// Delete the key if its value is null.
				v.SetMapIndex(key, reflect.Value{})
				continue
			}

			// Use recursion to go deeper.
			filterNulls(val)
		}

	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			// Use recursion to go deeper.
			filterNulls(v.Index(i))
		}
	}
}
