package service

import "errors"

var ErrUnauthenticatedSession = errors.New("service: unauthenticated session")
var ErrUnsupportedFrame = errors.New("service: unsupported frame")
var ErrSendNotReady = errors.New("service: send not ready")
