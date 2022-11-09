package npool

import "time"

type Conn[T Element] struct {
	p          *Pool[T]
	e          T
	t          time.Time
	created    time.Time
	next, prev *Conn[T]
}

// Element 获取实际对象
func (pc *Conn[T]) Element() T {
	return pc.e
}

// Close 将该对象关闭
func (pc *Conn[T]) Close() error {
	return pc.p.put(pc, true)
}

// Release 将该对象放回连接池
func (pc *Conn[T]) Release() {
	pc.p.put(pc, false)
}
