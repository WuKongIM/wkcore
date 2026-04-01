package service

import "time"

type Options struct {
	Now func() time.Time
}
