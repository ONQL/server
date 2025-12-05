package common

import "errors"

var (
	ErrNotFound      = errors.New("not found")
	ErrDuplicate     = errors.New("duplicate entry")
	ErrInvalidInput  = errors.New("invalid input")
	ErrDatabaseExists = errors.New("database already exists")
	ErrTableExists    = errors.New("table already exists")
)
