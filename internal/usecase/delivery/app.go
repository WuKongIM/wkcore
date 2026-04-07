package delivery

type Options struct {
	Runtime Runtime
}

type App struct {
	runtime Runtime
}

func New(opts Options) *App {
	if opts.Runtime == nil {
		opts.Runtime = noopRuntime{}
	}
	return &App{runtime: opts.Runtime}
}

type noopRuntime struct{}
