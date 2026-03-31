package transport

type ListenerOptions struct {
	Name    string
	Network string
	Address string
	Path    string
	OnError func(error)
}
