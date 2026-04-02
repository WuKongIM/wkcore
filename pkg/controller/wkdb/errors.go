package wkdb

import "errors"

var (
	ErrNotFound         = errors.New("wkdb: not found")
	ErrAlreadyExists    = errors.New("wkdb: already exists")
	ErrChecksumMismatch = errors.New("wkdb: checksum mismatch")
	ErrCorruptValue     = errors.New("wkdb: corrupt value")
	ErrInvalidArgument  = errors.New("wkdb: invalid argument")
)
