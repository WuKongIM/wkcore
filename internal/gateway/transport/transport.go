package transport

type Factory interface {
	Name() string
	// Build must return one listener per input spec, preserving spec order in the returned slice.
	Build(specs []ListenerSpec) ([]Listener, error)
}

type Listener interface {
	Start() error
	Stop() error
	Addr() string
}

type Conn interface {
	ID() uint64
	Write([]byte) error
	Close() error
	LocalAddr() string
	RemoteAddr() string
}

type ConnHandler interface {
	OnOpen(conn Conn) error
	OnData(conn Conn, data []byte) error
	OnClose(conn Conn, err error)
}

type ListenerSpec struct {
	Options ListenerOptions
	Handler ConnHandler
}
