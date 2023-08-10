package internal

import "golang.org/x/xerrors"

var (
	ErrRequestCanceled = xerrors.New("request canceled")
	ErrItemNotFound    = xerrors.New("item not found")
)
