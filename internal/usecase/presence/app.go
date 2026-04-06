package presence

import "time"

type Options struct {
	Now func() time.Time
}

type App struct {
	dir *directory
	now func() time.Time
}

func New(opts Options) *App {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &App{
		dir: newDirectory(),
		now: opts.Now,
	}
}
