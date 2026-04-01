package service

import "errors"

var ErrUnauthenticatedSession = errors.New("service: unauthenticated session")
