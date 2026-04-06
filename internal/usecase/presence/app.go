package presence

type Options struct{}

type App struct {
	dir *directory
}

func New(opts Options) *App {
	_ = opts
	return &App{
		dir: newDirectory(),
	}
}
