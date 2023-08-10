package internal

type Handler interface {
	PreHandler
	Path() string
	Receiver() interface{}
	Namespaces() map[string]interface{}
}
