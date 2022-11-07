package npool

type Conn[T Element] interface {
	Element() T

	Close() error

	Release()
}
