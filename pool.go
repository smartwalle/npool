package npool

// Copyright 2012 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

// This is a fork of https://github.com/gomodule/redigo/blob/master/redis/pool.go written in a
// more extendable way

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrPoolExhausted = errors.New("connection pool exhausted")
var ErrClosedPool = errors.New("get on closed pool")

type options struct {
	// Maximum number of idle connections in the pool.
	maxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	maxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	idleTimeout time.Duration

	// If wait is true and the pool is at the maxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	wait bool

	// Close connections older than this duration. If the value is zero, then
	// the pool does not close connections based on age.
	maxLifetime time.Duration
}

type Option func(opts *options)

func WithMaxIdle(idle int) Option {
	return func(opts *options) {
		opts.maxIdle = idle
	}
}

func WithMaxActive(active int) Option {
	return func(opts *options) {
		opts.maxActive = active
	}
}

func WithIdleTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.idleTimeout = timeout
	}
}

func WithWait(wait bool) Option {
	return func(opts *options) {
		opts.wait = wait
	}
}

func WithMaxLifetime(lifetime time.Duration) Option {
	return func(opts *options) {
		opts.maxLifetime = lifetime
	}
}

type Pool[T Element] struct {
	*options

	// Dial is an application supplied function for creating and configuring a
	// connection.
	dial func(ctx context.Context) (T, error)

	// TestOnBorrow is an optional application supplied function for checking
	// the health of an idle connection before the connection is used again by
	// the application. Argument t is the time that the connection was returned
	// to the pool. If the function returns an error, then the connection is
	// closed.
	TestOnBorrow func(ele T, t time.Time) error

	mu           sync.Mutex    // mu protects the following fields
	closed       bool          // set to true when the pool is closed.
	active       int           // the number of open connections in the pool
	initOnce     sync.Once     // the init ch once func
	ch           chan struct{} // limits open connections when p.wait is true
	idle         idleList[T]   // idle connections
	waitCount    int64         // total number of connections waited for.
	waitDuration time.Duration // total time waited for new connections.
}

// New creates a new pool.
func New[T Element](dial func(ctx context.Context) (T, error), opts ...Option) *Pool[T] {
	var nPool = &Pool[T]{}
	nPool.dial = dial
	nPool.options = &options{}
	for _, opt := range opts {
		if opt != nil {
			opt(nPool.options)
		}
	}
	return nPool
}

// Get gets a connection using the provided context.
//
// The provided Context must be non-nil. If the context expires before the
// connection is complete, an error is returned. Any expiration on the context
// will not affect the returned connection.
//
// If the function completes without error, then the application must close the
// returned connection.
func (p *Pool[T]) Get(ctx context.Context) (Conn[T], error) {
	// wait until there is a vacant connection in the pool.
	waited, err := p.waitVacantConn(ctx)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()

	if waited > 0 {
		p.waitCount++
		p.waitDuration += waited
	}

	// Prune stale connections at the back of the idle list.
	if p.idleTimeout > 0 {
		n := p.idle.count
		for i := 0; i < n && p.idle.back != nil && p.idle.back.t.Add(p.idleTimeout).Before(time.Now()); i++ {
			pc := p.idle.back
			p.idle.popBack()
			p.mu.Unlock()
			pc.e.Close()
			p.mu.Lock()
			p.active--
		}
	}

	// Get idle connection from the front of idle list.
	for p.idle.front != nil {
		pc := p.idle.front
		p.idle.popFront()
		p.mu.Unlock()
		if (p.TestOnBorrow == nil || p.TestOnBorrow(pc.e, pc.t) == nil) &&
			(p.maxLifetime == 0 || time.Now().Sub(pc.created) < p.maxLifetime) {
			return pc, nil
		}
		pc.e.Close()
		p.mu.Lock()
		p.active--
	}

	// Check for pool closed before dialing a new connection.
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosedPool
	}

	// Handle limit for p.wait == false.
	if !p.wait && p.maxActive > 0 && p.active >= p.maxActive {
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	}

	p.active++
	p.mu.Unlock()
	c, err := p.dial(ctx)
	if err != nil {
		p.mu.Lock()
		p.active--
		if p.ch != nil && !p.closed {
			p.ch <- struct{}{}
		}
		p.mu.Unlock()
		return nil, err
	}
	return &poolConn[T]{p: p, e: c, created: time.Now()}, nil
}

// Stats returns pool's statistics.
func (p *Pool[T]) Stats() Stats {
	p.mu.Lock()
	stats := Stats{
		ActiveCount:  p.active,
		IdleCount:    p.idle.count,
		WaitCount:    p.waitCount,
		WaitDuration: p.waitDuration,
	}
	p.mu.Unlock()

	return stats
}

// ActiveCount returns the number of connections in the pool. The count
// includes idle connections and connections in use.
func (p *Pool[T]) ActiveCount() int {
	p.mu.Lock()
	active := p.active
	p.mu.Unlock()
	return active
}

// IdleCount returns the number of idle connections in the pool.
func (p *Pool[T]) IdleCount() int {
	p.mu.Lock()
	idle := p.idle.count
	p.mu.Unlock()
	return idle
}

// Close releases the resources used by the pool.
func (p *Pool[T]) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.active -= p.idle.count
	pc := p.idle.front
	p.idle.count = 0
	p.idle.front, p.idle.back = nil, nil
	if p.ch != nil {
		close(p.ch)
	}
	p.mu.Unlock()
	for ; pc != nil; pc = pc.next {
		pc.e.Close()
	}
	return nil
}

func (p *Pool[T]) lazyInit() {
	p.initOnce.Do(func() {
		p.ch = make(chan struct{}, p.maxActive)
		if p.closed {
			close(p.ch)
		} else {
			for i := 0; i < p.maxActive; i++ {
				p.ch <- struct{}{}
			}
		}
	})
}

// waitVacantConn waits for a vacant connection in pool if waiting
// is enabled and pool size is limited, otherwise returns instantly.
// If ctx expires before that, an error is returned.
//
// If there were no vacant connection in the pool right away it returns the time spent waiting
// for that connection to appear in the pool.
func (p *Pool[T]) waitVacantConn(ctx context.Context) (waited time.Duration, err error) {
	if !p.wait || p.maxActive <= 0 {
		// No wait or no connection limit.
		return 0, nil
	}

	p.lazyInit()

	// wait indicates if we believe it will block so its not 100% accurate
	// however for stats it should be good enough.
	wait := len(p.ch) == 0
	var start time.Time
	if wait {
		start = time.Now()
	}

	select {
	case <-p.ch:
		// Additionally check that context hasn't expired while we were waiting,
		// because `select` picks a random `case` if several of them are "ready".
		select {
		case <-ctx.Done():
			p.ch <- struct{}{}
			return 0, ctx.Err()
		default:
		}
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	if wait {
		return time.Since(start), nil
	}
	return 0, nil
}

func (p *Pool[T]) put(pc *poolConn[T], forceClose bool) error {
	p.mu.Lock()
	if !p.closed && !forceClose {
		pc.t = time.Now()
		p.idle.pushFront(pc)
		if p.idle.count > p.maxIdle {
			pc = p.idle.back
			p.idle.popBack()
		} else {
			pc = nil
		}
	}

	var err error
	if pc != nil {
		p.mu.Unlock()
		err = pc.e.Close()
		p.mu.Lock()
		p.active--
	}

	if p.ch != nil && !p.closed {
		p.ch <- struct{}{}
	}
	p.mu.Unlock()
	return err
}

type poolConn[T Element] struct {
	p          *Pool[T]
	e          T
	t          time.Time
	created    time.Time
	next, prev *poolConn[T]
}

func (pc *poolConn[T]) Element() T {
	return pc.e
}

func (pc *poolConn[T]) Close() error {
	return pc.p.put(pc, true)
}

func (pc *poolConn[T]) Release() {
	pc.p.put(pc, false)
}

type idleList[T Element] struct {
	count       int
	front, back *poolConn[T]
}

func (l *idleList[T]) pushFront(pc *poolConn[T]) {
	pc.next = l.front
	pc.prev = nil
	if l.count == 0 {
		l.back = pc
	} else {
		l.front.prev = pc
	}
	l.front = pc
	l.count++
}

func (l *idleList[T]) popFront() {
	pc := l.front
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.next.prev = nil
		l.front = pc.next
	}
	pc.next, pc.prev = nil, nil
}

func (l *idleList[T]) popBack() {
	pc := l.back
	l.count--
	if l.count == 0 {
		l.front, l.back = nil, nil
	} else {
		pc.prev.next = nil
		l.back = pc.prev
	}
	pc.next, pc.prev = nil, nil
}
