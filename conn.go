package npool

type Conn[T Element] interface {
	Element() T

	// Close 将该对象关闭
	Close() error

	// Release 将该对象放回连接池
	Release()
}
