package main

import (
	"context"
	"fmt"
	"github.com/smartwalle/npool"
	"math/rand"
	"sync"
	"time"
)

type Conn struct {
	Id int
}

func (c *Conn) Close() error {
	return nil
}

func main() {
	var p = npool.New[*Conn](
		func(ctx context.Context) (*Conn, error) {
			var nc = &Conn{}
			nc.Id = rand.Int()
			return nc, nil
		},
		npool.WithMaxActive(2),
		npool.WithMaxIdle(2),
		npool.WithWait(true),
	)
	var w = &sync.WaitGroup{}

	for i := 0; i < 3; i++ {
		w.Add(1)
		go do(i, w, p)
	}

	w.Wait()
}

func do(key int, w *sync.WaitGroup, p *npool.Pool[*Conn]) {
	for i := 0; i < 10; i++ {
		var c, err = p.Get(context.Background())
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(c.Element().Id, key, "di...")
		time.Sleep(time.Second * 1)
		c.Release()
	}
	w.Done()
}
