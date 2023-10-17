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
func (c *Conn[T]) Element() T {
	return c.e
}

// Close 将该对象关闭
func (c *Conn[T]) Close() error {
	return c.p.put(c, true)
}

// Release 将该对象放回连接池
func (c *Conn[T]) Release() {
	c.p.put(c, false)
}
